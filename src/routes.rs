use axum::{
    extract::{Query, State},
    response::Json,
};
use serde::Deserialize;
use serde_json::json;
use ct_merkle::RootHash;
use sha2::{Sha256, digest::Output};
use base64::Engine;

use crate::{LeafHash, MerkleProof, ConsistencyProof};

// Type alias for our JSON responses
type JsonResponse = Json<serde_json::Value>;

#[derive(Debug, Deserialize)]
pub struct ProofQuery {
    #[serde(with = "crate::proof_bytes_format")]
    pub hash: Vec<u8>,
}

#[derive(Debug, Deserialize)]
pub struct ConsistencyQuery {
    /// The root hash of the historical tree to prove consistency with
    #[serde(with = "crate::proof_bytes_format")]
    pub old_root: Vec<u8>,
}


// Fetches all entries from the database to rebuild the merkle tree
pub async fn fetch_all_entries(
    client: &tokio_postgres::Client,
) -> Result<(Vec<LeafHash>, Vec<(Vec<u8>, usize)>, i64), tokio_postgres::Error> {
    println!("🔄 Fetching all entries from database...");
    
    // Query all rows from processed_log which guarantees sequential order
    let rows = client
        .query(
            "SELECT pl.id, pl.leaf_hash 
             FROM processed_log pl 
             ORDER BY pl.id", 
            &[])
        .await?;
    
    // Create vectors for both the hashes and the index mappings
    let mut leaf_hashes = Vec::new();
    let mut index_mappings = Vec::new();
    let mut last_id = 0;
    
    // Collect all entries and mappings - now based only on leaf_hash
    for (idx, row) in rows.into_iter().enumerate() {
        let id = row.get::<_, i64>("id");
        let hash = row.get::<_, Vec<u8>>("leaf_hash");
        
        leaf_hashes.push(LeafHash { hash: hash.clone() });
        index_mappings.push((hash, idx));
        last_id = id;
    }
    
    println!("✅ Fetched {} entries from database", leaf_hashes.len());
    Ok((leaf_hashes, index_mappings, last_id))
}

// Handler for the /root endpoint
pub async fn get_merkle_root(
    State(state): State<crate::AppState>,
) -> JsonResponse {
    let merkle_state = state.merkle_state.read();
    let root = merkle_state.tree.root();
    Json(json!({
        "merkle_root": base64::engine::general_purpose::STANDARD.encode(root.as_bytes()),
        "tree_size": merkle_state.tree.len(),
        "last_processed_id": merkle_state.last_processed_id,
        "status": "ok"
    }))
}

// Handler for the /proof endpoint that generates inclusion proofs
pub async fn get_inclusion_proof(
    query_result: Result<Query<ProofQuery>, axum::extract::rejection::QueryRejection>,
    State(state): State<crate::AppState>,
) -> JsonResponse {
    // Handle query parameter parsing errors
    let Query(query) = match query_result {
        Ok(query) => query,
        Err(e) => {
            println!("🚫 Invalid proof request: {}", e);
            return Json(json!({
                "status": "error",
                "error": format!(
                    "Invalid request parameters: {}. The hash parameter must be base64 encoded.", 
                    e
                )
            }));
        }
    };
    let merkle_state = state.merkle_state.read();
    
    // Look up the index in our O(1) index map
    let found_idx = state.index_map.get(&query.hash);
    
    // Create LeafHash from provided hash
    let query_hash = LeafHash { hash: query.hash };
    
    match found_idx {
        Some(idx) => {
            // Generate the proof
            let proof = merkle_state.tree.prove_inclusion(idx);
            let root = merkle_state.tree.root();
            
            // Verify the proof immediately as a sanity check
            match root.verify_inclusion(&query_hash, idx as u64, &proof) {
                Ok(()) => {
                    // Create a MerkleProof with all necessary data
                    let merkle_proof = MerkleProof {
                        index: idx,
                        proof_bytes: proof.as_bytes().iter().copied().collect(),
                        root: root.as_bytes().to_vec(),
                        tree_size: merkle_state.tree.len() as usize,
                    };
                    
                    Json(json!({
                        "status": "ok",
                        "proof": merkle_proof
                    }))
                },
                Err(e) => Json(json!({
                    "status": "error",
                    "error": format!("Generated proof failed verification: {}", e)
                }))
            }
        },
        None => Json(json!({
            "status": "error",
            "error": "Hash not found in the tree"
        }))
    }
}

// Handler for the /rebuild endpoint
#[axum::debug_handler]
pub async fn trigger_rebuild(
    State(state): State<crate::AppState>,
) -> JsonResponse {
    match crate::rebuild_tree(&state).await {
        Ok((size, root, last_id)) => Json(json!({
            "status": "ok",
            "tree_size": size,
            "merkle_root": base64::engine::general_purpose::STANDARD.encode(root),
            "last_processed_id": last_id,
        })),
        Err(e) => Json(json!({
            "status": "error",
            "error": e.to_string()
        }))
    }
}

// Handler for the /consistency endpoint that generates consistency proofs
pub async fn get_consistency_proof(
    query_result: Result<Query<ConsistencyQuery>, axum::extract::rejection::QueryRejection>,
    State(state): State<crate::AppState>,
) -> JsonResponse {
    // Handle query parameter parsing errors
    let Query(query) = match query_result {
        Ok(query) => query,
        Err(e) => {
            println!("🚫 Invalid consistency proof request: {}", e);
            return Json(json!({
                "status": "error",
                "error": format!(
                    "Invalid request parameters: {}. The old_root parameter must be base64 encoded.", 
                    e
                )
            }));
        }
    };
    // Look up size for historical root
    let old_size = match state.root_map.get_size(&query.old_root) {
        Some(size) => size,
        None => return Json(json!({
            "status": "error",
            "error": "Historical root hash not found in tree history"
        }))
    };

    // Get current tree info
    let merkle_state = state.merkle_state.read();
    let current_size = merkle_state.tree.len() as usize;
    let current_root = merkle_state.tree.root();

    // Validate the sizes
    if old_size > current_size {
        return Json(json!({
            "status": "error",
            "error": format!("Invalid request: Historical tree size {} is larger than current size {}", 
                           old_size, current_size)
        }));
    }

    if old_size == 0 {
        return Json(json!({
            "status": "error",
            "error": "Invalid request: Cannot generate consistency proof for empty tree"
        }));
    }

    if old_size == current_size {
        return Json(json!({
            "status": "error", 
            "error": "Invalid request: No new entries since historical root"
        }));
    }
    
    // Calculate how many items were added since historical root
    let num_additions = current_size - old_size;

    // Create RootHash objects for verification
    let mut old_digest = Output::<Sha256>::default();
    old_digest.copy_from_slice(&query.old_root);
    let old_root = RootHash::<Sha256>::new(old_digest, old_size as u64);
    
    // Generate consistency proof using current tree state
    let proof = merkle_state.tree.prove_consistency(num_additions);
    
    if let Err(e) = current_root.verify_consistency(&old_root, &proof) {
        println!("Consistency proof verification failed: {}", e);
        return Json(json!({
            "status": "error",
            "error": format!("Generated proof failed verification: {}", e)
        }));
    }

    // Return the verified proof
    Json(json!({
        "status": "ok",
        "proof": ConsistencyProof {
            old_tree_size: old_size,
            old_root: query.old_root,
            proof_bytes: proof.as_bytes().iter().copied().collect(),
            new_root: current_root.as_bytes().to_vec(),
            new_tree_size: current_size,
        }
    }))
}