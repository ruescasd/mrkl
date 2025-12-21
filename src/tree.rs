use crate::LeafHash;
use ct_merkle::ConsistencyProof;
use ct_merkle::InclusionProof;
use ct_merkle::mem_backed_tree::MemoryBackedTree;
use sha2::Sha256;
use std::collections::HashMap;

/// Error type for proof operations
#[derive(Debug)]
pub enum ProofError {
    LeafNotFound,
    RootNotFound,
    LeafNotPresentAtRoot,
    InvalidRootOrder,
    SameRoot,
    UnexpectedRoot(Vec<u8>),
}

/// Error type for tree rewinding operations
#[derive(Debug)]
pub enum RewindError {
    RootNotFound,
    UnexpectedRoot(Vec<u8>),
}

/// A merkle tree implementation based on Certificate Transparency that maintains root history and provides proofs
pub struct CtMerkleTree {
    tree: MemoryBackedTree<Sha256, LeafHash>,
    /// Maps leaf hash to its position (index) in the tree
    leaf_hash_to_index: HashMap<Vec<u8>, usize>,
    /// Maps root hash to the tree size that produced it
    root_hash_to_size: HashMap<Vec<u8>, usize>,
}

impl CtMerkleTree {
    /// Creates a new empty merkle tree
    pub fn new() -> Self {
        Self {
            tree: MemoryBackedTree::new(),
            leaf_hash_to_index: HashMap::new(),
            root_hash_to_size: HashMap::new(),
        }
    }

    /// Adds a new leaf to the tree and updates the associated maps
    pub fn push(&mut self, leaf: LeafHash) {
        let idx = self.tree.len();
        self.leaf_hash_to_index
            .insert(leaf.hash.clone(), idx.try_into().unwrap());
        self.tree.push(leaf);
        let root = self.tree.root();
        self.root_hash_to_size.insert(
            root.as_bytes().to_vec(),
            self.tree.len().try_into().unwrap(),
        );
    }

    /// Gets the current root hash
    pub fn root(&self) -> Vec<u8> {
        self.tree.root().as_bytes().to_vec()
    }

    /// Gets the current number of leaves in the tree
    pub fn len(&self) -> usize {
        self.tree.len().try_into().unwrap()
    }

    /// Returns true if the tree is empty
    pub fn is_empty(&self) -> bool {
        self.tree.len() == 0
    }

    /// Generates an inclusion proof for a given leaf hash against the current root
    pub fn prove_inclusion(&self, hash: &[u8]) -> Result<InclusionProof<Sha256>, ProofError> {
        self.prove_inclusion_at_root(hash, &self.root())
    }

    /// Generates a consistency proof between the current tree and a historical root
    pub fn prove_consistency(
        &self,
        old_root: &[u8],
    ) -> Result<ConsistencyProof<Sha256>, ProofError> {
        self.prove_consistency_between(old_root, &self.root())
    }

    /// Verifies an inclusion proof for a given leaf hash against the current root
    pub fn verify_inclusion(
        &self,
        leaf_hash: &[u8],
        proof: &InclusionProof<Sha256>,
    ) -> Result<bool, ct_merkle::InclusionVerifError> {
        self.verify_inclusion_at_root(leaf_hash, &self.root(), proof)
    }

    /// Verifies a consistency proof between a historical root and the current root
    pub fn verify_consistency(
        &self,
        old_root: &[u8],
        proof: &ConsistencyProof<Sha256>,
    ) -> Result<bool, ct_merkle::ConsistencyVerifError> {
        self.verify_consistency_between(old_root, &self.root(), proof)
    }

    /// Gets the index for a given leaf hash
    pub fn get_index(&self, hash: &[u8]) -> Option<usize> {
        self.leaf_hash_to_index.get(hash).copied()
    }

    /// Gets the tree size for a given root hash
    pub fn get_size_for_root(&self, root: &[u8]) -> Option<usize> {
        self.root_hash_to_size.get(root).copied()
    }

    /// Returns a tree corresponding to the historical state at the given root hash.
    pub(crate) fn rewind(
        &self,
        historical_root: &[u8],
    ) -> Result<MemoryBackedTree<Sha256, LeafHash>, RewindError> {
        // Get size from root hash to size map
        let size = self
            .root_hash_to_size
            .get(historical_root)
            .ok_or(RewindError::RootNotFound)?;

        // Create new tree
        let mut historical_tree = MemoryBackedTree::new();

        // Take first 'size' leaves from current tree
        for leaf in self.tree.items().iter().take(*size) {
            historical_tree.push(leaf.clone());
        }

        // Get root bytes for comparison
        let root_bytes = historical_tree.root().as_bytes().to_vec();

        // Verify we got the expected root
        if root_bytes != historical_root {
            return Err(RewindError::UnexpectedRoot(root_bytes));
        }

        Ok(historical_tree)
    }

    /// Generates an inclusion proof for a given leaf hash at a specific historical root state
    pub fn prove_inclusion_at_root(
        &self,
        leaf_hash: &[u8],
        root: &[u8],
    ) -> Result<InclusionProof<Sha256>, ProofError> {
        // Get index for the leaf
        let idx = self
            .leaf_hash_to_index
            .get(leaf_hash)
            .ok_or(ProofError::LeafNotFound)?;

        // Get historical tree state
        let historical_tree = self.rewind(root).map_err(|e| match e {
            RewindError::RootNotFound => ProofError::RootNotFound,
            RewindError::UnexpectedRoot(r) => ProofError::UnexpectedRoot(r),
        })?;

        // Check if leaf is actually in the historical state
        if *idx >= historical_tree.len().try_into().unwrap() {
            return Err(ProofError::LeafNotPresentAtRoot);
        }

        // Generate proof from historical state
        Ok(historical_tree.prove_inclusion(*idx))
    }

    /// Verifies an inclusion proof for a given leaf hash against a specific historical root
    pub fn verify_inclusion_at_root(
        &self,
        leaf_hash: &[u8],
        root: &[u8],
        proof: &InclusionProof<Sha256>,
    ) -> Result<bool, ct_merkle::InclusionVerifError> {
        // Get index for this leaf hash
        let index = match self.leaf_hash_to_index.get(leaf_hash) {
            Some(idx) => *idx,
            None => return Ok(false),
        };

        // Get historical tree state
        let historical_tree = match self.rewind(root) {
            Ok(tree) => tree,
            Err(_) => return Ok(false),
        };

        // Check if leaf is in the historical state
        if index >= historical_tree.len().try_into().unwrap() {
            return Ok(false);
        }

        // Create leaf hash wrapper
        let leaf = LeafHash::new(leaf_hash.to_vec());

        // Get historical root and verify against it
        let historical_root = historical_tree.root();
        match historical_root.verify_inclusion(&leaf, index as u64, proof) {
            Ok(_) => Ok(true),
            Err(ct_merkle::InclusionVerifError::MalformedProof) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Generates a consistency proof between any two historical roots
    pub fn prove_consistency_between(
        &self,
        old_root: &[u8],
        new_root: &[u8],
    ) -> Result<ConsistencyProof<Sha256>, ProofError> {
        // Get sizes for both roots
        let old_size = *self
            .root_hash_to_size
            .get(old_root)
            .ok_or(ProofError::RootNotFound)?;
        let new_size = *self
            .root_hash_to_size
            .get(new_root)
            .ok_or(ProofError::RootNotFound)?;

        // Check for same root
        if old_root == new_root {
            return Err(ProofError::SameRoot);
        }

        // New root must be from a larger tree
        if new_size <= old_size {
            return Err(ProofError::InvalidRootOrder);
        }

        // Get tree state at new_root
        let new_tree = self.rewind(new_root).map_err(|e| match e {
            RewindError::RootNotFound => ProofError::RootNotFound,
            RewindError::UnexpectedRoot(r) => ProofError::UnexpectedRoot(r),
        })?;

        // Calculate number of additions
        let num_additions = new_size - old_size;

        // Generate proof from the historical tree state
        Ok(new_tree.prove_consistency(num_additions.try_into().unwrap()))
    }

    /// Verifies a consistency proof between any two historical roots
    pub fn verify_consistency_between(
        &self,
        old_root: &[u8],
        new_root: &[u8],
        proof: &ConsistencyProof<Sha256>,
    ) -> Result<bool, ct_merkle::ConsistencyVerifError> {
        // Get sizes for both roots
        let old_size = match self.root_hash_to_size.get(old_root) {
            Some(size) => *size,
            None => return Ok(false),
        };

        // Rewind to new root state
        let new_tree = match self.rewind(new_root) {
            Ok(tree) => tree,
            Err(_) => return Ok(false),
        };

        // Create root hash from old root bytes
        let mut old_digest = sha2::digest::Output::<Sha256>::default();
        old_digest.copy_from_slice(old_root);
        let old_root_hash = ct_merkle::RootHash::new(old_digest, old_size as u64);

        // Verify the proof using the new tree's root
        match new_tree.root().verify_consistency(&old_root_hash, proof) {
            Ok(_) => Ok(true),
            Err(ct_merkle::ConsistencyVerifError::MalformedProof) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_tree() -> CtMerkleTree {
        let mut tree = CtMerkleTree::new();
        // Add a few leaves
        let leaf1 = LeafHash::new(vec![1, 2, 3]);
        let leaf2 = LeafHash::new(vec![4, 5, 6]);
        let leaf3 = LeafHash::new(vec![7, 8, 9]);

        tree.push(leaf1.clone());
        tree.push(leaf2.clone());
        tree.push(leaf3.clone());

        tree
    }

    #[test]
    fn test_valid_inclusion_proof() {
        let tree = create_test_tree();
        let leaf_hash = vec![1, 2, 3];

        // Generate proof for first leaf
        let proof = tree
            .prove_inclusion(&leaf_hash)
            .expect("Should create proof");

        // Verify the proof
        let result = tree
            .verify_inclusion(&leaf_hash, &proof)
            .expect("Should verify");
        assert!(result, "Valid inclusion proof should verify successfully");
    }

    #[test]
    fn test_invalid_inclusion_proof() {
        let tree = create_test_tree();
        let leaf_hash = vec![1, 2, 3];
        let wrong_hash = vec![9, 9, 9];

        // Generate proof for first leaf
        let proof = tree
            .prove_inclusion(&leaf_hash)
            .expect("Should create proof");

        // Try to verify proof with wrong leaf hash
        let result = tree
            .verify_inclusion(&wrong_hash, &proof)
            .expect("Should process verify request");
        assert!(!result, "Invalid inclusion proof should fail verification");
    }

    #[test]
    fn test_valid_consistency_proof() {
        let mut tree = CtMerkleTree::new();

        // Add first leaf and save root
        let leaf1 = LeafHash::new(vec![1, 2, 3]);
        tree.push(leaf1);
        let old_root = tree.root();

        // Add more leaves
        let leaf2 = LeafHash::new(vec![4, 5, 6]);
        tree.push(leaf2);

        // Generate and verify consistency proof
        let proof = tree
            .prove_consistency(&old_root)
            .expect("Should create proof");
        let result = tree
            .verify_consistency(&old_root, &proof)
            .expect("Should verify");
        assert!(result, "Valid consistency proof should verify successfully");
    }

    #[test]
    fn test_invalid_consistency_proof() {
        let mut tree = CtMerkleTree::new();

        // Add first leaf and save root
        let leaf1 = LeafHash::new(vec![1, 2, 3]);
        tree.push(leaf1);
        let first_root = tree.root();

        // Add second leaf to get a different root
        let leaf2 = LeafHash::new(vec![4, 5, 6]);
        tree.push(leaf2);

        // Create fake root
        let fake_root = vec![9; 32]; // Wrong root of correct length

        // Generate proof using first root
        let proof = tree
            .prove_consistency(&first_root)
            .expect("Should create proof");

        // Try to verify with fake root
        let result = tree
            .verify_consistency(&fake_root, &proof)
            .expect("Should process verify request");
        assert!(
            !result,
            "Consistency proof with wrong root should fail verification"
        );
    }

    #[test]
    fn test_consistency_proof_with_current_root() {
        let mut tree = CtMerkleTree::new();

        // Add a few leaves to create a non-empty tree
        tree.push(LeafHash::new(vec![1, 2, 3]));
        tree.push(LeafHash::new(vec![4, 5, 6]));

        // Get current root
        let current_root = tree.root();

        // Try to generate a proof using current root (this should fail)
        let result = tree.prove_consistency(&current_root);

        // Should get SameRoot error
        assert!(
            matches!(result, Err(ProofError::SameRoot)),
            "Proving consistency with current root should return SameRoot error"
        );
    }

    #[test]
    fn test_rewind() {
        let mut tree = CtMerkleTree::new();

        // Add three leaves and save roots after each
        let leaf1 = LeafHash::new(vec![1, 2, 3]);
        tree.push(leaf1.clone());
        let root1 = tree.root();

        let leaf2 = LeafHash::new(vec![4, 5, 6]);
        tree.push(leaf2.clone());
        let root2 = tree.root();

        let leaf3 = LeafHash::new(vec![7, 8, 9]);
        tree.push(leaf3);

        // Try to rewind to second root
        let historical_tree = tree.rewind(&root2).expect("Should rewind to second root");
        assert_eq!(
            historical_tree.len(),
            2,
            "Historical tree should have 2 leaves"
        );
        assert_eq!(
            historical_tree.root().as_bytes().to_vec(),
            root2,
            "Historical tree should have correct root"
        );

        // Try to rewind to first root
        let historical_tree = tree.rewind(&root1).expect("Should rewind to first root");
        assert_eq!(
            historical_tree.len(),
            1,
            "Historical tree should have 1 leaf"
        );
        assert_eq!(
            historical_tree.root().as_bytes().to_vec(),
            root1,
            "Historical tree should have correct root"
        );

        // Try to rewind to non-existent root
        let fake_root = vec![9; 32];
        assert!(matches!(
            tree.rewind(&fake_root),
            Err(RewindError::RootNotFound)
        ));
    }

    #[test]
    fn test_inclusion_at_root() {
        let mut tree = CtMerkleTree::new();

        // Add three leaves and save roots after each
        let leaf1_hash = vec![1, 2, 3];
        tree.push(LeafHash::new(leaf1_hash.clone()));
        let root1 = tree.root();

        let leaf2_hash = vec![4, 5, 6];
        tree.push(LeafHash::new(leaf2_hash.clone()));
        let root2 = tree.root();

        let leaf3_hash = vec![7, 8, 9];
        tree.push(LeafHash::new(leaf3_hash.clone()));

        // Test proving/verifying inclusion at various historical roots

        // For leaf1 at root1 (initial state)
        let proof1 = tree
            .prove_inclusion_at_root(&leaf1_hash, &root1)
            .expect("Should create proof");
        assert!(
            tree.verify_inclusion_at_root(&leaf1_hash, &root1, &proof1)
                .expect("Should verify")
        );

        // For leaf1 at root2 (still present)
        let proof2 = tree
            .prove_inclusion_at_root(&leaf1_hash, &root2)
            .expect("Should create proof");
        assert!(
            tree.verify_inclusion_at_root(&leaf1_hash, &root2, &proof2)
                .expect("Should verify")
        );

        // For leaf2 at root1 (wasn't present yet)
        assert!(matches!(
            tree.prove_inclusion_at_root(&leaf2_hash, &root1),
            Err(ProofError::LeafNotPresentAtRoot)
        ));

        // For leaf2 at root2 (just added)
        let proof3 = tree
            .prove_inclusion_at_root(&leaf2_hash, &root2)
            .expect("Should create proof");
        assert!(
            tree.verify_inclusion_at_root(&leaf2_hash, &root2, &proof3)
                .expect("Should verify")
        );

        // For non-existent root
        let fake_root = vec![9; 32];
        assert!(matches!(
            tree.prove_inclusion_at_root(&leaf1_hash, &fake_root),
            Err(ProofError::RootNotFound)
        ));
        assert!(
            tree.verify_inclusion_at_root(&leaf1_hash, &fake_root, &proof1)
                .expect("Should return false")
                == false
        );

        // For incorrect proof (using proof from different root)
        assert!(
            tree.verify_inclusion_at_root(&leaf1_hash, &root2, &proof1)
                .expect("Should process request")
                == false
        );
    }

    #[test]
    fn test_consistency_between_roots() {
        let mut tree = CtMerkleTree::new();

        // Add three leaves and save roots after each
        let leaf1 = LeafHash::new(vec![1, 2, 3]);
        tree.push(leaf1);
        let root1 = tree.root();

        let leaf2 = LeafHash::new(vec![4, 5, 6]);
        tree.push(leaf2);
        let root2 = tree.root();

        let leaf3 = LeafHash::new(vec![7, 8, 9]);
        tree.push(leaf3);
        let root3 = tree.root();

        // Test valid consistency proofs between roots

        // Between root1 and root2
        let proof1 = tree
            .prove_consistency_between(&root1, &root2)
            .expect("Should create proof between root1 and root2");
        assert!(
            tree.verify_consistency_between(&root1, &root2, &proof1)
                .expect("Should verify between root1 and root2")
        );

        // Between root1 and root3
        let proof2 = tree
            .prove_consistency_between(&root1, &root3)
            .expect("Should create proof between root1 and root3");
        assert!(
            tree.verify_consistency_between(&root1, &root3, &proof2)
                .expect("Should verify between root1 and root3")
        );

        // Between root2 and root3
        let proof3 = tree
            .prove_consistency_between(&root2, &root3)
            .expect("Should create proof between root2 and root3");
        assert!(
            tree.verify_consistency_between(&root2, &root3, &proof3)
                .expect("Should verify between root2 and root3")
        );

        // Test invalid cases

        // Reversed order (new_root before old_root)
        assert!(matches!(
            tree.prove_consistency_between(&root2, &root1),
            Err(ProofError::InvalidRootOrder)
        ));

        // Same root
        assert!(matches!(
            tree.prove_consistency_between(&root1, &root1),
            Err(ProofError::SameRoot)
        ));

        // Non-existent root
        let fake_root = vec![9; 32];
        assert!(matches!(
            tree.prove_consistency_between(&root1, &fake_root),
            Err(ProofError::RootNotFound)
        ));
        assert!(
            tree.verify_consistency_between(&root1, &fake_root, &proof1)
                .expect("Should return false")
                == false
        );

        // Wrong proof (using proof between different roots)
        assert!(
            tree.verify_consistency_between(&root1, &root3, &proof1)
                .expect("Should return false")
                == false
        );
    }
}
