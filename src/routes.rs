use axum::{
    extract::{Query, State},
    response::Json,
};
use serde::{Serialize, Deserialize};
use serde_json::json;
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};

use crate::{LeafHash, InclusionProof, ConsistencyProof};

// Type alias for our JSON responses
type JsonResponse = Json<serde_json::Value>;

#[derive(Debug, Serialize, Deserialize)]
pub struct InclusionQuery {
    #[serde(with = "crate::routes::proof_bytes_format")]
    pub hash: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsistencyQuery {
    /// The root hash of the historical tree to prove consistency with
    #[serde(with = "crate::routes::proof_bytes_format")]
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
        "merkle_root": base64::engine::general_purpose::STANDARD.encode(&root),
        "tree_size": merkle_state.tree.len(),
        "last_processed_id": merkle_state.last_processed_id,
        "status": "ok"
    }))
}

// Handler for the /proof endpoint that generates inclusion proofs
pub async fn get_inclusion_proof(
    query_result: Result<Query<InclusionQuery>, axum::extract::rejection::QueryRejection>,
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
    let tree = &merkle_state.tree;
    
    // Try to generate and verify proof using our new tree's method
    let proof = match tree.prove_inclusion(&query.hash) {
        Ok(proof) => proof,
        Err(e) => return Json(json!({
            "status": "error",
            "error": format!("Failed to generate inclusion proof: {:?}", e)
        }))
    };
    
    // Verify the proof as a sanity check
    match tree.verify_inclusion(&query.hash, &proof) {
        Ok(true) => {
            // Get the index (safe because prove_inclusion succeeded)
            let index = tree.get_index(&query.hash).unwrap();

            // Create InclusionProof with all necessary data
            let inclusion_proof = InclusionProof {
                index,
                proof_bytes: proof.as_bytes().iter().copied().collect(),
                root: tree.root(),
                tree_size: tree.len(),
            };

            Json(json!({
                "status": "ok",
                "proof": inclusion_proof
            }))
        },
        Ok(false) | Err(_) => Json(json!({
            "status": "error",
            "error": "Generated proof failed verification"
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
    let merkle_state = state.merkle_state.read();
    let tree = &merkle_state.tree;

    // Try to generate consistency proof
    let proof = match tree.prove_consistency(&query.old_root) {
        Ok(proof) => proof,
        Err(e) => return Json(json!({
            "status": "error",
            "error": format!("Failed to generate consistency proof: {:?}", e)
        }))
    };

    // Verify the proof
    match tree.verify_consistency(&query.old_root, &proof) {
        Ok(true) => {
            let old_size = tree.get_size_for_root(&query.old_root).unwrap(); // Safe because prove_consistency succeeded
            
            // Return the verified proof
            Json(json!({
                "status": "ok",
                "proof": ConsistencyProof {
                    old_tree_size: old_size,
                    old_root: query.old_root,
                    proof_bytes: proof.as_bytes().iter().copied().collect(),
                    new_root: tree.root(),
                    new_tree_size: tree.len(),
                }
            }))
        },
        Ok(false) | Err(_) => Json(json!({
            "status": "error",
            "error": "Generated proof failed verification"
        }))
    }
}

// Custom serialization for byte arrays to use base64
pub mod proof_bytes_format {
    use serde::{Serializer, Deserializer};
    use super::*;

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&BASE64.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        String::deserialize(deserializer)
            .and_then(|string| BASE64.decode(string.as_bytes())
                .map_err(|err| Error::custom(err.to_string())))
    }
}