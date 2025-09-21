use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use ct_merkle::HashableLeaf;
use digest::Update;
use serde::{Serialize, Deserialize};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};

// Export test client module
pub mod test_client;

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

// Custom serialization for byte arrays to use base64
mod proof_bytes_format {
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

    /// Creates a LeafHash by computing the SHA-256 hash of data
    pub fn from_data(data: &str) -> Self {
        use sha2::{Sha256, Digest};
        let mut hasher = Sha256::new();
        Digest::update(&mut hasher, data.as_bytes());
        Self {
            hash: hasher.finalize().to_vec()
        }
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

/// A thread-safe mapping of data strings to their indices in the merkle tree.
/// This allows O(1) lookup of proof indices without storing strings in the tree.
#[derive(Clone)]
pub struct IndexMap {
    map: Arc<RwLock<HashMap<String, usize>>>,
}

impl IndexMap {
    /// Creates a new empty index map.
    pub fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Records a mapping between data and its index.
    pub fn insert(&self, data: String, index: usize) {
        self.map.write().insert(data, index);
    }

    /// Looks up the index for a piece of data.
    pub fn get(&self, data: &str) -> Option<usize> {
        self.map.read().get(data).copied()
    }

    /// Clears all mappings.
    pub fn clear(&self) {
        self.map.write().clear();
    }
}