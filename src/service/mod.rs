use std::sync::Arc;
use dashmap::DashMap;

pub mod client;
pub mod processor;
pub mod routes;
pub mod server;
pub mod state;

pub use client::Client;
pub use processor::rebuild_all_logs;
pub use routes::{
    ConsistencyQuery, InclusionQuery, get_consistency_proof,
    get_inclusion_proof, get_log_size, get_merkle_root,
};
pub use server::{create_server, initialize_app_state, run_server};
pub use state::MerkleState;

/// Configuration for a source table that feeds into the merkle log
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// The name of the source table in the database
    pub table_name: String,
    /// The column containing the pre-computed hash value
    pub hash_column: String,
    /// The column used for ordering (e.g., "id", "created_at")
    pub order_column: String,
    /// The log this source belongs to
    pub log_name: String,
}

impl SourceConfig {
    /// Creates a new source configuration
    pub fn new(table_name: impl Into<String>, hash_column: impl Into<String>, order_column: impl Into<String>, log_name: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            hash_column: hash_column.into(),
            order_column: order_column.into(),
            log_name: log_name.into(),
        }
    }
}

/// Shared state between HTTP server and periodic processor
#[derive(Clone)]
pub struct AppState {
    /// Map of log name to merkle state (tree + last processed ID)
    /// DashMap allows concurrent access without a single global lock
    pub merkle_states: Arc<DashMap<String, Arc<parking_lot::RwLock<MerkleState>>>>,
    /// Database connection pool for operations that need it
    pub db_pool: deadpool_postgres::Pool,
}
