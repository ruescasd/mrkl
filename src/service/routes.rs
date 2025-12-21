use axum::{
    extract::{Path, Query, State},
    response::Json,
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::AppState;
use crate::{ConsistencyProof, InclusionProof};

// HTTP handlers - read-only access to in-memory DashMap state
// All database operations are handled exclusively by the batch processor
// This ensures clean separation: HTTP handlers never block on database I/O

// Type alias for our JSON responses
type JsonResponse = Json<serde_json::Value>;

#[derive(Debug, Serialize, Deserialize)]
pub struct InclusionQuery {
    #[serde(with = "crate::service::routes::proof_bytes_format")]
    pub hash: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsistencyQuery {
    /// The root hash of the historical tree to prove consistency with
    #[serde(with = "crate::service::routes::proof_bytes_format")]
    pub old_root: Vec<u8>,
}

// Handler for the /logs/{log_name}/root endpoint
pub async fn get_merkle_root(
    Path(log_name): Path<String>,
    State(state): State<AppState>,
) -> JsonResponse {
    // Get or create the merkle state for this log
    let merkle_state_arc = match state.merkle_states.get(&log_name) {
        Some(arc) => arc.clone(),
        None => {
            return Json(json!({
                "status": "error",
                "error": format!("Log '{}' not found", log_name)
            }));
        }
    };

    let merkle_state = merkle_state_arc.read();
    if merkle_state.tree.len() == 0 {
        return Json(json!({
            "status": "error",
            "error": "Merkle tree is empty"
        }));
    }
    let root = merkle_state.tree.root();
    Json(json!({
        "merkle_root": base64::engine::general_purpose::STANDARD.encode(&root),
        "tree_size": merkle_state.tree.len(),
        "last_processed_id": merkle_state.last_processed_id,
        "log_name": log_name,
        "status": "ok"
    }))
}

// Handler for the /logs/{log_name}/size endpoint
pub async fn get_log_size(
    Path(log_name): Path<String>,
    State(state): State<AppState>,
) -> JsonResponse {
    // Get the merkle state for this log
    let merkle_state_arc = match state.merkle_states.get(&log_name) {
        Some(arc) => arc.clone(),
        None => {
            // Log doesn't exist yet - return size 0
            return Json(json!({
                "status": "ok",
                "log_name": log_name,
                "size": 0
            }));
        }
    };

    let merkle_state = merkle_state_arc.read();
    Json(json!({
        "status": "ok",
        "log_name": log_name,
        "size": merkle_state.tree.len()
    }))
}

// Handler for the /logs/{log_name}/proof endpoint that generates inclusion proofs
pub async fn get_inclusion_proof(
    Path(log_name): Path<String>,
    query_result: Result<Query<InclusionQuery>, axum::extract::rejection::QueryRejection>,
    State(state): State<AppState>,
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

    // Get the merkle state for this log
    let merkle_state_arc = match state.merkle_states.get(&log_name) {
        Some(arc) => arc.clone(),
        None => {
            return Json(json!({
                "status": "error",
                "error": format!("Log '{}' not found", log_name)
            }));
        }
    };

    let merkle_state = merkle_state_arc.read();

    // Look up the index in our O(1) index map
    let tree = &merkle_state.tree;

    // Try to generate and verify proof using our new tree's method
    let proof = match tree.prove_inclusion(&query.hash) {
        Ok(proof) => proof,
        Err(e) => {
            return Json(json!({
                "status": "error",
                "error": format!("Failed to generate inclusion proof: {:?}", e)
            }));
        }
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
                "log_name": log_name,
                "proof": inclusion_proof
            }))
        }
        Ok(false) | Err(_) => Json(json!({
            "status": "error",
            "error": "Generated proof failed verification"
        })),
    }
}

// Handler for the /logs/{log_name}/consistency endpoint that generates consistency proofs
pub async fn get_consistency_proof(
    Path(log_name): Path<String>,
    query_result: Result<Query<ConsistencyQuery>, axum::extract::rejection::QueryRejection>,
    State(state): State<AppState>,
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

    // Get the merkle state for this log
    let merkle_state_arc = match state.merkle_states.get(&log_name) {
        Some(arc) => arc.clone(),
        None => {
            return Json(json!({
                "status": "error",
                "error": format!("Log '{}' not found", log_name)
            }));
        }
    };

    let merkle_state = merkle_state_arc.read();
    let tree = &merkle_state.tree;

    // Try to generate consistency proof
    let proof = match tree.prove_consistency(&query.old_root) {
        Ok(proof) => proof,
        Err(e) => {
            return Json(json!({
                "status": "error",
                "error": format!("Failed to generate consistency proof: ProofError: {:?}", e)
            }));
        }
    };

    // Verify the proof
    match tree.verify_consistency(&query.old_root, &proof) {
        Ok(true) => {
            let old_size = tree.get_size_for_root(&query.old_root).unwrap(); // Safe because prove_consistency succeeded

            // Return the verified proof
            Json(json!({
                "status": "ok",
                "log_name": log_name,
                "proof": ConsistencyProof {
                    old_tree_size: old_size,
                    old_root: query.old_root,
                    proof_bytes: proof.as_bytes().iter().copied().collect(),
                    new_root: tree.root(),
                    new_tree_size: tree.len(),
                }
            }))
        }
        Ok(false) | Err(_) => Json(json!({
            "status": "error",
            "error": "Generated proof failed verification"
        })),
    }
}

// Custom serialization for byte arrays to use base64
pub mod proof_bytes_format {
    use super::*;
    use serde::{Deserializer, Serializer};

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
        String::deserialize(deserializer).and_then(|string| {
            BASE64
                .decode(string.as_bytes())
                .map_err(|err| Error::custom(err.to_string()))
        })
    }
}


