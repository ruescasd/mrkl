use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use ct_merkle::{HashableLeaf, RootHash};
use digest::{Update};
use sha2::{Sha256, digest::Output};
use serde::{Serialize, Deserialize};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};

// Export test client module
pub mod test_client;

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