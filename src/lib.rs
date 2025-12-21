use anyhow::Result;
use ct_merkle::{ConsistencyProof as CtConsistencyProof, InclusionProof as CtInclusionProof};
use ct_merkle::{HashableLeaf, RootHash};
use digest::Update;
use serde::{Deserialize, Serialize};
use sha2::{Sha256, digest::Output};

// Export modules
pub mod service;
pub mod tree;

// Re-export service layer components
pub use service::{
    AppState, ConsistencyQuery, InclusionQuery, MerkleState,
    get_consistency_proof, get_inclusion_proof, get_log_size, get_merkle_root,
};

/// A wrapper around a merkle consistency proof that proves one tree is a prefix of another
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsistencyProof {
    /// The number of leaves in the old (smaller) tree
    pub old_tree_size: usize,
    /// The root hash of the old tree
    pub old_root: Vec<u8>,
    /// The consistency proof path bytes
    pub proof_bytes: Vec<u8>,
    /// The root hash of the new (larger) tree
    pub new_root: Vec<u8>,
    /// The total number of leaves in the new tree
    pub new_tree_size: usize,
}

impl ConsistencyProof {
    /// Verifies this proof using ct-merkle's consistency proof verification
    pub fn verify(&self) -> Result<bool, ct_merkle::ConsistencyVerifError> {
        // Create digest from old root bytes
        let mut old_digest = Output::<Sha256>::default();
        if self.old_root.len() != old_digest.len() {
            return Err(ct_merkle::ConsistencyVerifError::MalformedProof);
        }
        old_digest.copy_from_slice(&self.old_root);

        // Create old root hash with digest and size
        let old_root = RootHash::<Sha256>::new(old_digest, self.old_tree_size as u64);

        // Create digest from new root bytes
        let mut new_digest = Output::<Sha256>::default();
        if self.new_root.len() != new_digest.len() {
            return Err(ct_merkle::ConsistencyVerifError::MalformedProof);
        }
        new_digest.copy_from_slice(&self.new_root);

        // Create new root hash with digest and size
        let new_root = RootHash::<Sha256>::new(new_digest, self.new_tree_size as u64);

        // Create consistency proof from stored bytes
        let proof = CtConsistencyProof::<Sha256>::try_from_bytes(self.proof_bytes.clone())?;

        // Verify consistency
        new_root.verify_consistency(&old_root, &proof).map(|_| true)
    }
}

/// A wrapper around a merkle inclusion proof with metadata needed for external verification
#[derive(Debug, Serialize, Deserialize)]
pub struct InclusionProof {
    /// The index of the leaf in the tree
    pub index: usize,
    /// The root hash at the time the proof was generated
    pub root: Vec<u8>,
    /// The inclusion proof path as raw bytes
    pub proof_bytes: Vec<u8>,
    /// The total number of leaves in the tree at the time of proof generation
    pub tree_size: usize,
}
impl InclusionProof {
    /// Verifies this proof against the given leaf hash using ct-merkle's proof verification
    pub fn verify(&self, hash: &[u8]) -> Result<bool> {
        // Create the leaf hash from the provided hash
        let leaf_hash = LeafHash::new(hash.to_vec());

        // Create a digest from our stored root bytes
        let mut digest = Output::<Sha256>::default();
        if self.root.len() != digest.len() {
            return Err(anyhow::anyhow!("Invalid root hash length"));
        }
        digest.copy_from_slice(&self.root);

        // Create the root hash with the digest and actual tree size
        let root_hash = RootHash::<Sha256>::new(digest, self.tree_size as u64);

        // Create the inclusion proof from our stored bytes
        let proof = CtInclusionProof::<Sha256>::from_bytes(self.proof_bytes.clone());

        // Verify using root's verification method
        match root_hash.verify_inclusion(&leaf_hash, self.index as u64, &proof) {
            Ok(()) => Ok(true),
            Err(_) => Ok(false),
        }
    }
}

/// Represents a pre-computed hash value for the merkle tree.
/// This type only stores the hash, reducing memory usage in the tree.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeafHash {
    /// The pre-computed SHA-256 hash of some data
    pub hash: Vec<u8>,
}

impl LeafHash {
    /// Creates a new LeafHash from a given hash value
    pub fn new(hash: Vec<u8>) -> Self {
        Self { hash }
    }

    /// Returns the raw hash bytes
    pub fn as_bytes(&self) -> &[u8] {
        &self.hash
    }
}

impl HashableLeaf for LeafHash {
    fn hash<H: Update>(&self, hasher: &mut H) {
        hasher.update(&self.hash);
    }
}
