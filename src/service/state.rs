use crate::{LeafHash, tree::CtMerkleTree};
use dashmap::DashMap;
use std::sync::Arc;

/// Shared state between HTTP server and periodic processor
#[derive(Clone)]
pub struct AppState {
    /// Map of log name to merkle state (tree + last processed ID)
    /// DashMap allows concurrent access without a single global lock
    pub merkle_states: Arc<DashMap<String, Arc<parking_lot::RwLock<MerkleState>>>>,
    /// Database connection pool for operations that need it
    pub db_pool: deadpool_postgres::Pool,
}

/// A combined state holding the merkle tree and its corresponding last processed ID.
/// This ensures that readers always see a consistent view of both values.
/// This is a service-layer wrapper around the core CtMerkleTree to coordinate with HTTP endpoints.
pub struct MerkleState {
    /// The history-tracking merkle tree storing leaf hashes and their indices
    pub tree: CtMerkleTree,
    /// The ID of the last processed log entry incorporated into the tree
    pub last_processed_id: i64,
}

impl MerkleState {
    /// Creates a new empty merkle state
    pub fn new() -> Self {
        Self {
            tree: CtMerkleTree::new(),
            last_processed_id: 0,
        }
    }

    /// Updates both the tree and last_processed_id atomically
    pub fn update_with_entry(&mut self, hash: LeafHash, id: i64) {
        self.tree.push(hash);
        self.last_processed_id = id;
    }

    
    // for the moment we must checkpoint after each addition,
    // because in a rebuild scenario we do not know which roots have been published
    // these checkpoints are currently part of update_with_entry, and
    // cannot yet be called separately.
    /*
    /// Set a root checkpoint for the current tree state
     pub fn root_checkpoint(&mut self) {
        self.tree.root_checkpoint();
    }*/
}
