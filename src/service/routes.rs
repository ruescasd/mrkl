use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};

use crate::service::responses::{
    ApiError, ApiResponse, ConsistencyProofResponse, HasLeafResponse, HasRootResponse,
    InclusionProofResponse, RootResponse, SizeResponse, MetricsResponse,
    LogMetricsResponse, GlobalMetricsResponse,
};
use crate::service::state::AppState;
use crate::{ConsistencyProof, InclusionProof};

// HTTP handlers - read-only access to in-memory DashMap state
// All database operations are handled exclusively by the batch processor
// This ensures clean separation: HTTP handlers never block on database I/O

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

#[derive(Debug, Serialize, Deserialize)]
pub struct HasLeafQuery {
    /// The leaf hash to check for
    #[serde(with = "crate::service::routes::proof_bytes_format")]
    pub hash: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct HasRootQuery {
    /// The root hash to check for
    #[serde(with = "crate::service::routes::proof_bytes_format")]
    pub root: Vec<u8>,
}

// Handler for the /logs/{log_name}/root endpoint
pub async fn get_merkle_root(
    Path(log_name): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // Get or create the merkle state for this log
    let merkle_state_arc = match state.merkle_states.get(&log_name) {
        Some(arc) => arc.clone(),
        None => {
            return ApiError::LogNotFound(log_name).to_response();
        }
    };

    let merkle_state = merkle_state_arc.read();
    if merkle_state.tree.len() == 0 {
        return ApiError::EmptyTree(log_name).to_response();
    }
    let root = merkle_state.tree.root();

    ApiResponse::success(RootResponse {
        log_name,
        merkle_root: base64::engine::general_purpose::STANDARD.encode(&root),
        tree_size: merkle_state.tree.len() as u64,
        last_processed_id: merkle_state.last_processed_id,
    })
}

// Handler for the /logs/{log_name}/size endpoint
pub async fn get_log_size(
    Path(log_name): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // Get the merkle state for this log
    let size = match state.merkle_states.get(&log_name) {
        Some(arc) => {
            let merkle_state = arc.read();
            merkle_state.tree.len()
        }
        None => {
            // Log doesn't exist yet - return size 0
            0
        }
    };

    ApiResponse::success(SizeResponse { log_name, size: size as u64 })
}

// Handler for the /logs/{log_name}/proof endpoint that generates inclusion proofs
pub async fn get_inclusion_proof(
    Path(log_name): Path<String>,
    query_result: Result<Query<InclusionQuery>, axum::extract::rejection::QueryRejection>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // Handle query parameter parsing errors
    let Query(query) = match query_result {
        Ok(query) => query,
        Err(e) => {
            println!("🚫 Invalid proof request: {}", e);
            return ApiError::InvalidRequest(format!(
                "Invalid request parameters: {}. The hash parameter must be base64 encoded.",
                e
            ))
            .to_response();
        }
    };

    // Get the merkle state for this log
    let merkle_state_arc = match state.merkle_states.get(&log_name) {
        Some(arc) => arc.clone(),
        None => {
            return ApiError::LogNotFound(log_name).to_response();
        }
    };

    let merkle_state = merkle_state_arc.read();

    // Look up the index in our O(1) index map
    let tree = &merkle_state.tree;

    // Try to generate and verify proof using our new tree's method
    let proof = match tree.prove_inclusion(&query.hash) {
        Ok(proof) => proof,
        Err(e) => {
            return ApiError::ProofGenerationFailed(format!(
                "Failed to generate inclusion proof: {:?}",
                e
            ))
            .to_response();
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

            ApiResponse::success(InclusionProofResponse {
                log_name,
                proof: inclusion_proof,
            })
        }
        Ok(false) | Err(_) => ApiError::ProofVerificationFailed.to_response(),
    }
}

// Handler for the /logs/{log_name}/consistency endpoint that generates consistency proofs
pub async fn get_consistency_proof(
    Path(log_name): Path<String>,
    query_result: Result<Query<ConsistencyQuery>, axum::extract::rejection::QueryRejection>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // Handle query parameter parsing errors
    let Query(query) = match query_result {
        Ok(query) => query,
        Err(e) => {
            println!("🚫 Invalid consistency proof request: {}", e);
            return ApiError::InvalidRequest(format!(
                "Invalid request parameters: {}. The old_root parameter must be base64 encoded.",
                e
            ))
            .to_response();
        }
    };

    // Get the merkle state for this log
    let merkle_state_arc = match state.merkle_states.get(&log_name) {
        Some(arc) => arc.clone(),
        None => {
            return ApiError::LogNotFound(log_name).to_response();
        }
    };

    let merkle_state = merkle_state_arc.read();
    let tree = &merkle_state.tree;

    // Try to generate consistency proof
    let proof = match tree.prove_consistency(&query.old_root) {
        Ok(proof) => proof,
        Err(e) => {
            return ApiError::ProofGenerationFailed(format!(
                "Failed to generate consistency proof: ProofError: {:?}",
                e
            ))
            .to_response();
        }
    };

    // Verify the proof
    match tree.verify_consistency(&query.old_root, &proof) {
        Ok(true) => {
            let old_size = tree.get_size_for_root(&query.old_root).unwrap(); // Safe because prove_consistency succeeded

            // Return the verified proof
            let consistency_proof = ConsistencyProof {
                old_tree_size: old_size,
                old_root: query.old_root,
                proof_bytes: proof.as_bytes().iter().copied().collect(),
                new_root: tree.root(),
                new_tree_size: tree.len(),
            };

            ApiResponse::success(ConsistencyProofResponse {
                log_name,
                proof: consistency_proof,
            })
        }
        Ok(false) | Err(_) => ApiError::ProofVerificationFailed.to_response(),
    }
}

// Handler for the /logs/{log_name}/has_leaf endpoint that checks if a leaf exists
pub async fn has_leaf(
    Path(log_name): Path<String>,
    query_result: Result<Query<HasLeafQuery>, axum::extract::rejection::QueryRejection>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // Handle query parameter parsing errors
    let Query(query) = match query_result {
        Ok(query) => query,
        Err(e) => {
            println!("🚫 Invalid has_leaf request: {}", e);
            return ApiError::InvalidRequest(format!(
                "Invalid request parameters: {}. The hash parameter must be base64 encoded.",
                e
            ))
            .to_response();
        }
    };

    // Get the merkle state for this log
    let merkle_state_arc = match state.merkle_states.get(&log_name) {
        Some(arc) => arc.clone(),
        None => {
            return ApiError::LogNotFound(log_name).to_response();
        }
    };

    let merkle_state = merkle_state_arc.read();
    let tree = &merkle_state.tree;

    // Check if the leaf exists using O(1) index lookup
    let exists = tree.get_index(&query.hash).is_some();

    ApiResponse::success(HasLeafResponse { log_name, exists })
}

// Handler for the /logs/{log_name}/has_root endpoint that checks if a root exists
pub async fn has_root(
    Path(log_name): Path<String>,
    query_result: Result<Query<HasRootQuery>, axum::extract::rejection::QueryRejection>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    // Handle query parameter parsing errors
    let Query(query) = match query_result {
        Ok(query) => query,
        Err(e) => {
            println!("🚫 Invalid has_root request: {}", e);
            return ApiError::InvalidRequest(format!(
                "Invalid request parameters: {}. The root parameter must be base64 encoded.",
                e
            ))
            .to_response();
        }
    };

    // Get the merkle state for this log
    let merkle_state_arc = match state.merkle_states.get(&log_name) {
        Some(arc) => arc.clone(),
        None => {
            return ApiError::LogNotFound(log_name).to_response();
        }
    };

    let merkle_state = merkle_state_arc.read();
    let tree = &merkle_state.tree;

    // Check if the root exists using O(1) root lookup
    let exists = tree.get_size_for_root(&query.root).is_some();

    ApiResponse::success(HasRootResponse { log_name, exists })
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

/// Get current metrics for all logs and global statistics
pub async fn metrics(
    State(app_state): State<AppState>,
) -> Result<impl IntoResponse, ApiResponse<()>> {
    let metrics = app_state.metrics.get_snapshot();
    
    let mut logs = std::collections::HashMap::new();
    for (log_name, log_metrics) in metrics.0 {
        logs.insert(log_name, LogMetricsResponse {
            last_batch_rows: log_metrics.last_batch_rows,
            last_batch_leaves: log_metrics.last_batch_leaves,
            last_total_ms: log_metrics.last_total_ms,
            last_copy_ms: log_metrics.last_copy_ms,
            last_query_sources_ms: log_metrics.last_query_sources_ms,
            last_insert_merkle_log_ms: log_metrics.last_insert_merkle_log_ms,
            last_fetch_merkle_log_ms: log_metrics.last_fetch_merkle_log_ms,
            last_tree_update_ms: log_metrics.last_tree_update_ms,
            total_batches: log_metrics.batches_processed,
            tree_size: log_metrics.tree_size,
            tree_memory_bytes: log_metrics.tree_memory_bytes,
            last_update: log_metrics.last_updated.to_rfc3339(),
        });
    }
    
    let global = GlobalMetricsResponse {
        last_cycle_duration_ms: metrics.1.last_cycle_duration_ms,
        last_active_log_count: metrics.1.last_active_log_count,
        last_cycle_fraction: metrics.1.last_cycle_fraction,
    };
    
    Ok(ApiResponse::success(MetricsResponse { logs, global }))
}
// Admin endpoints for processor control
// These endpoints allow graceful pause/resume/stop of the batch processor

/// Response for admin control endpoints
#[derive(Debug, Serialize)]
pub struct AdminControlResponse {
    pub message: String,
    pub state: String,
}

/// Pause the batch processor (stops processing but keeps server running)
pub async fn admin_pause(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    app_state.processor_state.store(1, std::sync::atomic::Ordering::Relaxed);
    ApiResponse::success(AdminControlResponse {
        message: "Batch processor paused".to_string(),
        state: "paused".to_string(),
    })
}

/// Resume the batch processor from paused state
pub async fn admin_resume(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    app_state.processor_state.store(0, std::sync::atomic::Ordering::Relaxed);
    ApiResponse::success(AdminControlResponse {
        message: "Batch processor resumed".to_string(),
        state: "running".to_string(),
    })
}

/// Stop the batch processor (will exit the processing loop)
pub async fn admin_stop(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    app_state.processor_state.store(2, std::sync::atomic::Ordering::Relaxed);
    ApiResponse::success(AdminControlResponse {
        message: "Batch processor stopping (HTTP server will continue)".to_string(),
        state: "stopping".to_string(),
    })
}

/// Get current processor state
pub async fn admin_status(
    State(app_state): State<AppState>,
) -> impl IntoResponse {
    let state_value = app_state.processor_state.load(std::sync::atomic::Ordering::Relaxed);
    let state_name = match state_value {
        1 => "paused",
        2 => "stopping",
        _ => "running",
    };
    
    ApiResponse::success(AdminControlResponse {
        message: format!("Batch processor is {}", state_name),
        state: state_name.to_string(),
    })
}