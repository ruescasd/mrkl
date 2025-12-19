use std::sync::Arc;

pub mod client;
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

/// Shared state between HTTP server and periodic processor
#[derive(Clone)]
pub struct AppState {
    /// The merkle state containing both the tree and its last processed ID
    pub merkle_state: Arc<parking_lot::RwLock<MerkleState>>,
    /// Database client for operations that need it
    pub client: Arc<tokio_postgres::Client>,
}
