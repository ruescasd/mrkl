use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use crate::{ConsistencyProof, InclusionProof};

/// Unified API response wrapper
/// 
/// All endpoints return either Success(data) or Error(error_info)
/// This ensures consistent JSON structure across the API
#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "lowercase")]
pub enum ApiResponse<T> {
    #[serde(rename = "ok")]
    Success(T),
    Error {
        error: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        error_type: Option<String>,
    },
}

impl<T: Serialize> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self::Success(data)
    }

    pub fn error(error: impl Into<String>) -> Self {
        Self::Error {
            error: error.into(),
            error_type: None,
        }
    }

    pub fn error_with_type(error: impl Into<String>, error_type: impl Into<String>) -> Self {
        Self::Error {
            error: error.into(),
            error_type: Some(error_type.into()),
        }
    }
}

/// Convert ApiResponse to Axum response with appropriate status code
impl<T: Serialize> IntoResponse for ApiResponse<T> {
    fn into_response(self) -> Response {
        let status = match &self {
            ApiResponse::Success(_) => StatusCode::OK,
            ApiResponse::Error { .. } => StatusCode::BAD_REQUEST,
        };

        (status, Json(self)).into_response()
    }
}

/// Typed error responses for different failure scenarios
#[derive(Debug, Clone)]
pub enum ApiError {
    LogNotFound(String),
    EmptyTree(String),
    InvalidRequest(String),
    ProofGenerationFailed(String),
    ProofVerificationFailed,
}

impl ApiError {
    /// Convert error to ApiResponse with appropriate error message
    pub fn to_response<T: Serialize>(self) -> ApiResponse<T> {
        match self {
            ApiError::LogNotFound(log_name) => ApiResponse::error_with_type(
                format!("Log '{}' not found", log_name),
                "log_not_found",
            ),
            ApiError::EmptyTree(log_name) => ApiResponse::error_with_type(
                format!("Merkle tree for log '{}' is empty", log_name),
                "empty_tree",
            ),
            ApiError::InvalidRequest(msg) => {
                ApiResponse::error_with_type(msg, "invalid_request")
            }
            ApiError::ProofGenerationFailed(msg) => {
                ApiResponse::error_with_type(msg, "proof_generation_failed")
            }
            ApiError::ProofVerificationFailed => ApiResponse::error_with_type(
                "Generated proof failed verification",
                "proof_verification_failed",
            ),
        }
    }
}

/// Response for GET /logs/{log_name}/root
#[derive(Debug, Serialize, Deserialize)]
pub struct RootResponse {
    pub log_name: String,
    pub merkle_root: String, // base64 encoded
    pub tree_size: u64,
    pub last_processed_id: i64,
}

/// Response for GET /logs/{log_name}/size
#[derive(Debug, Serialize, Deserialize)]
pub struct SizeResponse {
    pub log_name: String,
    pub size: u64,
}

/// Response for GET /logs/{log_name}/proof (inclusion proof)
#[derive(Debug, Serialize, Deserialize)]
pub struct InclusionProofResponse {
    pub log_name: String,
    pub proof: InclusionProof,
}

/// Response for GET /logs/{log_name}/consistency (consistency proof)
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsistencyProofResponse {
    pub log_name: String,
    pub proof: ConsistencyProof,
}

/// Response for GET /logs/{log_name}/has_leaf
#[derive(Debug, Serialize, Deserialize)]
pub struct HasLeafResponse {
    pub log_name: String,
    pub exists: bool,
}

/// Response for GET /logs/{log_name}/has_root
#[derive(Debug, Serialize, Deserialize)]
pub struct HasRootResponse {
    pub log_name: String,
    pub exists: bool,
}

/// Per-log metrics snapshot
#[derive(Debug, Serialize)]
pub struct LogMetricsResponse {
    pub last_batch_rows: u64,
    pub last_batch_leaves: u64,
    pub last_total_ms: u64,
    pub last_copy_ms: u64,
    pub last_query_sources_ms: u64,
    pub last_insert_merkle_log_ms: u64,
    pub last_fetch_merkle_log_ms: u64,
    pub last_tree_update_ms: u64,
    pub total_batches: u64,
    pub tree_size: u64,
    pub tree_memory_bytes: u64,
    pub last_update: String,
}

/// Global metrics snapshot
#[derive(Debug, Serialize)]
pub struct GlobalMetricsResponse {
    pub last_cycle_duration_ms: u64,
    pub last_active_log_count: usize,
    pub last_cycle_fraction: f64,
}

/// Response for GET /metrics
#[derive(Debug, Serialize)]
pub struct MetricsResponse {
    pub logs: HashMap<String, LogMetricsResponse>,
    pub global: GlobalMetricsResponse,
}
