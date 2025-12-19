use crate::{LeafHash, tree::CtMerkleTree};

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
}
