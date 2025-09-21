use axum::{
    extract::{Query, State},
    response::Json,
};
use serde::Deserialize;
use serde_json::json;
use ct_merkle::RootHash;
use sha2::{Sha256, digest::Output};

use crate::{LeafHash, MerkleProof, ConsistencyProof};

// Type alias for our JSON responses
type JsonResponse = Json<serde_json::Value>;

#[derive(Debug, Deserialize)]
pub struct ProofQuery {
    pub data: String,
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
) -> Result<(Vec<LeafHash>, Vec<(String, usize)>, i32), tokio_postgres::Error> {
    println!("🔄 Fetching all entries from database...");
    
    // Query all rows ordered by id to ensure consistent order
    let rows = client
        .query("SELECT id, data, leaf_hash FROM append_only_log ORDER BY id", &[])
        .await?;
    
    // Create vectors for both the hashes and the index mappings
    let mut leaf_hashes = Vec::new();
    let mut index_mappings = Vec::new();
    let mut max_id = 0;
    
    // Collect all entries and mappings
    for (idx, row) in rows.into_iter().enumerate() {
        let id = row.get::<_, i32>("id");
        let data = row.get::<_, String>("data");
        let hash = row.get::<_, Vec<u8>>("leaf_hash");
        
        max_id = max_id.max(id);
        leaf_hashes.push(LeafHash { hash });
        index_mappings.push((data, idx));
    }
    
    println!("✅ Fetched {} entries from database", leaf_hashes.len());
    println!("📊 Maximum ID found: {}", max_id);
    Ok((leaf_hashes, index_mappings, max_id))
}

// Handler for the /root endpoint
pub async fn get_merkle_root(
    State(state): State<crate::AppState>,
) -> JsonResponse {
    let tree = state.merkle_tree.read();
    let root = tree.root();
    Json(json!({
        "merkle_root": format!("{:?}", root.as_bytes()),
        "tree_size": tree.len(),
        "status": "ok"
    }))
}

// Handler for the /proof endpoint that generates inclusion proofs
pub async fn get_inclusion_proof(
    Query(query): Query<ProofQuery>,
    State(state): State<crate::AppState>,
) -> JsonResponse {
    let tree = state.merkle_tree.read();
    
    // Look up the index in our O(1) index map
    let found_idx = state.index_map.get(&query.data);
    
    // First compute the hash of our query data
    let query_hash = LeafHash::from_data(&query.data);
    
    match found_idx {
        Some(idx) => {
            // Generate the proof
            let proof = tree.prove_inclusion(idx);
            let root = tree.root();
            
            // Verify the proof immediately as a sanity check
            match root.verify_inclusion(&query_hash, idx as u64, &proof) {
                Ok(()) => {
                    // Create a MerkleProof with all necessary data
                    let merkle_proof = MerkleProof {
                        index: idx,
                        proof_bytes: proof.as_bytes().iter().copied().collect(),
                        root: root.as_bytes().to_vec(),
                        tree_size: tree.len() as usize,
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
            "error": "Data not found in the tree"
        }))
    }
}

// Handler for the /rebuild endpoint
#[axum::debug_handler]
pub async fn trigger_rebuild(
    State(state): State<crate::AppState>,
) -> JsonResponse {
    match crate::rebuild_tree(&state).await {
        Ok((size, root)) => Json(json!({
            "status": "ok",
            "tree_size": size,
            "merkle_root": format!("{:?}", root),
        })),
        Err(e) => Json(json!({
            "status": "error",
            "error": e.to_string()
        }))
    }
}

// Handler for the /consistency endpoint that generates consistency proofs
pub async fn get_consistency_proof(
    Query(query): Query<ConsistencyQuery>,
    State(state): State<crate::AppState>,
) -> JsonResponse {
    // Look up size for historical root
    let old_size = match state.root_map.get_size(&query.old_root) {
        Some(size) => size,
        None => return Json(json!({
            "status": "error",
            "error": "Historical root hash not found in tree history"
        }))
    };

    // Get current tree info
    let tree = state.merkle_tree.read();
    let current_size = tree.len() as usize;
    let current_root = tree.root();

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
    let proof = tree.prove_consistency(num_additions);
    
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