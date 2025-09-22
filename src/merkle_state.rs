use ct_merkle::mem_backed_tree::MemoryBackedTree;
use sha2::Sha256;
use crate::LeafHash;

/// A combined state holding the merkle tree and its corresponding last processed ID.
/// This ensures that readers always see a consistent view of both values.
pub struct MerkleState {
    /// The memory-backed merkle tree storing just the leaf hashes
    pub tree: MemoryBackedTree<Sha256, LeafHash>,
    /// The ID of the last processed log entry incorporated into the tree
    pub last_processed_id: i64,
}

impl MerkleState {
    /// Creates a new empty merkle state
    pub fn new() -> Self {
        Self {
            tree: MemoryBackedTree::new(),
            last_processed_id: 0,
        }
    }
    
    /// Updates both the tree and last_processed_id atomically
    pub fn update_with_entry(&mut self, hash: LeafHash, id: i64) {
        self.tree.push(hash);
        self.last_processed_id = id;
    }
}