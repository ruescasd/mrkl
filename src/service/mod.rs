use std::sync::Arc;

pub mod client;
pub mod processor;
pub mod routes;
pub mod server;
pub mod state;

pub use client::Client;
pub use routes::{
    ConsistencyQuery, InclusionQuery, fetch_all_entries, get_consistency_proof,
    get_inclusion_proof, get_merkle_root, rebuild_tree, trigger_rebuild,
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
}

impl SourceConfig {
    /// Creates a new source configuration
    pub fn new(table_name: impl Into<String>, hash_column: impl Into<String>, order_column: impl Into<String>) -> Self {
        Self {
            table_name: table_name.into(),
            hash_column: hash_column.into(),
            order_column: order_column.into(),
        }
    }
}

/// Shared state between HTTP server and periodic processor
#[derive(Clone)]
pub struct AppState {
    /// The merkle state containing both the tree and its last processed ID
    pub merkle_state: Arc<parking_lot::RwLock<MerkleState>>,
    /// Database connection pool for operations that need it
    pub db_pool: deadpool_postgres::Pool,
    /// Configuration for source tables to process
    pub source_configs: Vec<SourceConfig>,
}
