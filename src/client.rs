use anyhow::Result;
use reqwest::Client as HttpClient;
use sha2::{Digest, Sha256};
use tokio_postgres::{NoTls};
use std::time::Duration;
use crate::{MerkleProof, ConsistencyProof};

pub struct Client {
    http_client: HttpClient,
    db_client: tokio_postgres::Client,
    api_base_url: String,
}

impl Client {
    /// Wait for a reasonable time to allow entries to be processed
    pub async fn wait_for_processing(&self) -> Result<()> {
        // Give enough time for the service to process entries
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        Ok(())
    }

    /// Creates a new test client that interacts with both the HTTP API and database
    pub async fn new(api_base_url: &str) -> Result<Self> {
        // Set up HTTP client with reasonable timeouts
        let http_client = HttpClient::builder()
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

    /// Gets a consistency proof proving that the current tree state is consistent with an older root hash
    pub async fn get_consistency_proof(&self, old_root: Vec<u8>) -> Result<ConsistencyProof> {
        // Base64 encode the old root hash for transmission
        let old_root_b64 = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &old_root);

        let response = self.http_client
            .get(&format!("{}/consistency", self.api_base_url))
            .query(&[("old_root", old_root_b64)])
            .send()
            .await?;
            
        // Parse the response JSON into our ConsistencyProof struct
        let response = response.json::<serde_json::Value>().await?;

        // Check for error response
        if response["status"].as_str() == Some("error") {
            return Err(anyhow::anyhow!(
                "Error getting consistency proof: {}",
                response["error"].as_str().unwrap_or("unknown error")
            ));
        }
        
        // Parse into our structured ConsistencyProof type
        let proof = serde_json::from_value(response["proof"].clone())?;
        Ok(proof)
    }

    /// Verifies that the current tree state is consistent with a previously observed state.
    /// Returns true if the current tree is a descendant of old_root (i.e., old tree is a prefix).
    pub async fn verify_tree_consistency(
        &self,
        old_root: Vec<u8>
    ) -> Result<bool> {
        // Get consistency proof between the old root and current state
        let proof = self.get_consistency_proof(old_root).await?;
        
        // Get current root to validate the proof matches current state
        let current_root = self.get_root().await?;
        if proof.new_root != current_root {
            return Ok(false);
        }
        
        // Verify the proof cryptographically
        proof.verify().map_err(|e| anyhow::anyhow!("Proof verification failed: {}", e))
    }
}