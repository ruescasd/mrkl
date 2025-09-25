use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use ct_merkle::{HashableLeaf, RootHash};
use ct_merkle::InclusionProof;
use digest::{Update};
use sha2::{Sha256, digest::Output};
use serde::{Serialize, Deserialize};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use anyhow::Result;

// Export modules
pub mod routes;
pub mod client;
pub mod merkle_state;
pub mod tree;

// Re-export route handlers and types
pub use routes::{
    get_merkle_root,
    get_inclusion_proof,
    get_consistency_proof,
    trigger_rebuild,
    fetch_all_entries,
};
pub use merkle_state::MerkleState;

// Re-export types used by handlers
pub use routes::{ProofQuery, ConsistencyQuery};

/// Shared state between HTTP server and periodic processor
#[derive(Clone)]
pub struct AppState {
    /// The merkle state containing both the tree and its last processed ID
    pub merkle_state: Arc<parking_lot::RwLock<MerkleState>>,
    /// Database client for operations that need it
    pub client: Arc<tokio_postgres::Client>,
}

/// A wrapper around a merkle consistency proof that proves one tree is a prefix of another
#[derive(Debug, Serialize, Deserialize)]
pub struct ConsistencyProof {
    /// The number of leaves in the old (smaller) tree
    pub old_tree_size: usize,
    /// The root hash of the old tree 
    #[serde(with = "proof_bytes_format")]
    pub old_root: Vec<u8>,
    /// The consistency proof path bytes
    #[serde(with = "proof_bytes_format")]
    pub proof_bytes: Vec<u8>,
    /// The root hash of the new (larger) tree
    #[serde(with = "proof_bytes_format")]
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
        let old_root = RootHash::<Sha256>::new(
            old_digest,
            self.old_tree_size as u64
        );

        // Create digest from new root bytes
        let mut new_digest = Output::<Sha256>::default();
        if self.new_root.len() != new_digest.len() {
            return Err(ct_merkle::ConsistencyVerifError::MalformedProof);
        }
        new_digest.copy_from_slice(&self.new_root);

        // Create new root hash with digest and size
        let new_root = RootHash::<Sha256>::new(
            new_digest,
            self.new_tree_size as u64
        );

        // Create consistency proof from stored bytes
        let proof = ct_merkle::ConsistencyProof::<Sha256>::try_from_bytes(self.proof_bytes.clone())?;

        // Verify consistency
        new_root.verify_consistency(&old_root, &proof).map(|_| true)
    }
}

/// A wrapper around a merkle inclusion proof with all data needed for verification
#[derive(Debug, Serialize, Deserialize)]
pub struct MerkleProof {
    /// The index of the leaf in the tree
    pub index: usize,
    /// The root hash at the time the proof was generated
    #[serde(with = "proof_bytes_format")]
    pub root: Vec<u8>,
    /// The inclusion proof path as raw bytes
    #[serde(with = "proof_bytes_format")]
    pub proof_bytes: Vec<u8>,
    /// The total number of leaves in the tree at the time of proof generation
    pub tree_size: usize,
}
impl MerkleProof {
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
        let root_hash = RootHash::<Sha256>::new(
            digest,
            self.tree_size as u64
        );
        
        // Create the inclusion proof from our stored bytes
        let proof = InclusionProof::<Sha256>::from_bytes(self.proof_bytes.clone());
        
        // Verify using root's verification method
        match root_hash.verify_inclusion(&leaf_hash, self.index as u64, &proof) {
            Ok(()) => Ok(true),
            Err(_) => Ok(false)
        }
    }
}

// Custom serialization for byte arrays to use base64
pub mod proof_bytes_format {
    use serde::{Serializer, Deserializer};
    use super::*;

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
        String::deserialize(deserializer)
            .and_then(|string| BASE64.decode(string.as_bytes())
                .map_err(|err| Error::custom(err.to_string())))
    }

    pub mod optional {
        use super::*;
        use serde::{Serializer, Deserializer};

        pub fn serialize<S>(bytes: &Option<Vec<u8>>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match bytes {
                Some(b) => super::serialize(b, serializer),
                None => serializer.serialize_none()
            }
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<Vec<u8>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            #[derive(Deserialize)]
            #[serde(untagged)]
            enum StringOrNull {
                String(String),
                Null,
            }

            match StringOrNull::deserialize(deserializer)? {
                StringOrNull::String(s) => {
                    BASE64.decode(s.as_bytes())
                        .map(Some)
                        .map_err(serde::de::Error::custom)
                },
                StringOrNull::Null => Ok(None),
            }
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

/// A thread-safe mapping of leaf hashes to their indices in the merkle tree.
/// This allows O(1) lookup of proof indices without storing additional data.
#[derive(Clone)]
pub struct HashIndexMap {
    map: Arc<RwLock<HashMap<Vec<u8>, usize>>>,
}

impl HashIndexMap {
    /// Creates a new empty index map.
    pub fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Records a mapping between a leaf hash and its index.
    pub fn insert(&self, hash: Vec<u8>, index: usize) {
        self.map.write().insert(hash, index);
    }

    /// Looks up the index for a leaf hash.
    pub fn get(&self, hash: &[u8]) -> Option<usize> {
        self.map.read().get(hash).copied()
    }

    /// Clears all mappings.
    pub fn clear(&self) {
        self.map.write().clear();
    }
}

/// A thread-safe mapping of root hashes to their corresponding tree sizes.
/// This allows finding the tree size for any historical root hash in O(1) time.
#[derive(Clone)]
pub struct RootMap {
    map: Arc<RwLock<HashMap<Vec<u8>, usize>>>,
}

impl RootMap {
    /// Creates a new empty root map.
    pub fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Records a mapping between a root hash and its tree size.
    pub fn insert(&self, root: Vec<u8>, size: usize) {
        self.map.write().insert(root, size);
    }

    /// Looks up the tree size for a given root hash.
    pub fn get_size(&self, root: &[u8]) -> Option<usize> {
        self.map.read().get(root).copied()
    }

    /// Clears all mappings.
    pub fn clear(&self) {
        self.map.write().clear();
    }
}

/// Rebuilds the merkle tree and associated maps from scratch using database entries.
/// Returns the number of entries, final root hash, and last processed ID on success.
pub async fn rebuild_tree(state: &AppState) -> anyhow::Result<(usize, Vec<u8>, i64)> {
    println!("🔄 Starting tree rebuild...");
    
    // Measure database fetch time
    let fetch_start = std::time::Instant::now();
    let (leaf_hashes, index_mappings, last_id) = routes::fetch_all_entries(&state.client).await?;
    let fetch_time = fetch_start.elapsed();
    println!("📥 Database fetch completed in {}ms", fetch_time.as_millis());
    
    // Measure computation time
    let compute_start = std::time::Instant::now();
    
    // Create new merkle state and build tree
    let mut merkle_state = MerkleState::new();
    
    // Add leaf hashes to the tree (this will update internal maps)
    for leaf_hash in leaf_hashes {
        merkle_state.update_with_entry(leaf_hash, last_id);
    }
    
    // Get final state
    let size = merkle_state.tree.len();
    let root = merkle_state.tree.root();
    
    let compute_time = compute_start.elapsed();
    let total_time = fetch_time + compute_time;
    println!("🧮 Tree computation completed in {}ms", compute_time.as_millis());
    println!("✅ Total rebuild time: {}ms for {} entries", total_time.as_millis(), size);
    
    // Update the merkle state
    *state.merkle_state.write() = merkle_state;

    Ok((size, root, last_id))
}