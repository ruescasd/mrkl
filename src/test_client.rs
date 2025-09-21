use anyhow::Result;
use reqwest::Client;
use sha2::{Digest, Sha256, digest::Output};
use tokio_postgres::{NoTls};
use std::time::Duration;
use ct_merkle::{InclusionProof, RootHash};
use crate::{LeafHash, MerkleProof};

pub struct TestClient {
    http_client: Client,
    db_client: tokio_postgres::Client,
    api_base_url: String,
}

impl MerkleProof {
    /// Verifies this proof against the given data using ct-merkle's proof verification
    pub fn verify(&self, data: &str) -> Result<bool> {
        // Create the leaf hash from the input data
        let leaf_hash = LeafHash::from_data(data);
        
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

impl TestClient {
    /// Creates a new test client that interacts with both the HTTP API and database
    pub async fn new(api_base_url: &str) -> Result<Self> {
        // Set up HTTP client with reasonable timeouts
        let http_client = Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        // Set up database connection
        let db_url = std::env::var("DATABASE_URL")
            .expect("DATABASE_URL must be set in .env file");
        let (db_client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(Self {
            http_client,
            db_client,
            api_base_url: api_base_url.to_string(),
        })
    }

    /// Adds a new entry to the log through the database
    pub async fn add_entry(&self, data: &str) -> Result<i32> {
        // First compute the hash
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        let hash_result = hasher.finalize();

        // Insert into database
        let row = self.db_client
            .query_one(
                "INSERT INTO append_only_log (data, leaf_hash) VALUES ($1, $2) RETURNING id",
                &[&data, &hash_result.as_slice()],
            )
            .await?;

        Ok(row.get(0))
    }

    /// Gets the current merkle root
    pub async fn get_root(&self) -> Result<Vec<u8>> {
        let response = self.http_client
            .get(&format!("{}/root", self.api_base_url))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        // Parse the root from the response
        let root_str = response["merkle_root"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid root response"))?;
        
        // Convert string representation to bytes
        let root_bytes = root_str
            .trim_matches(|c| c == '[' || c == ']')
            .split(',')
            .map(str::trim)
            .map(|s| s.parse::<u8>())
            .collect::<Result<Vec<_>, _>>()?;

        Ok(root_bytes)
    }

    /// Gets an inclusion proof for a piece of data
    pub async fn get_proof(&self, data: &str) -> Result<MerkleProof> {
        let response = self.http_client
            .get(&format!("{}/proof", self.api_base_url))
            .query(&[("data", data)])
            .send()
            .await?;
            
        // Parse the response JSON into our MerkleProof struct
        let response = response.json::<serde_json::Value>().await?;
        
        // Check for error response
        if response["status"].as_str() == Some("error") {
            return Err(anyhow::anyhow!(
                "Error getting proof: {}",
                response["error"].as_str().unwrap_or("unknown error")
            ));
        }
        
        // Parse into our structured MerkleProof type
        let proof = serde_json::from_value(response["proof"].clone())?;
        Ok(proof)
    }

    /// Rebuilds the merkle tree from scratch
    pub async fn trigger_rebuild(&self) -> Result<()> {
        let response = self.http_client
            .get(&format!("{}/rebuild", self.api_base_url))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        if response["status"].as_str() == Some("error") {
            return Err(anyhow::anyhow!(
                "Error triggering rebuild: {}",
                response["error"].as_str().unwrap_or("unknown error")
            ));
        }

        Ok(())
    }
}