use crate::{ConsistencyProof, InclusionProof};
use anyhow::Result;
use base64::Engine;
use reqwest::Client as HttpClient;
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio_postgres::NoTls;

pub struct Client {
    http_client: HttpClient,
    db_client: tokio_postgres::Client,
    api_base_url: String,
    default_log_name: String,
}

impl Client {
    /// Wait for a reasonable time to allow entries to be processed
    pub async fn wait_for_processing(&self) -> Result<()> {
        // Give enough time for the service to process entries
        tokio::time::sleep(std::time::Duration::from_millis(2000)).await;
        Ok(())
    }

    /// Gets the current size (number of entries) of the log
    pub async fn get_log_size(&self) -> Result<usize> {
        let response = self
            .http_client
            .get(&format!("{}/logs/{}/size", self.api_base_url, self.default_log_name))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        // Check for error response
        if response["status"].as_str() == Some("error") {
            return Err(anyhow::anyhow!(
                "Error getting log size: {}",
                response["error"].as_str().unwrap_or("unknown error")
            ));
        }

        let size = response["size"]
            .as_u64()
            .ok_or_else(|| anyhow::anyhow!("Invalid size response"))?;

        Ok(size as usize)
    }

    /// Waits until the log reaches the expected size by polling the size endpoint
    /// This is much more efficient than fixed-duration sleeps
    /// STRICT: Returns error if size exceeds expected (indicates entries from elsewhere)
    pub async fn wait_until_log_size(&self, expected_size: usize) -> Result<()> {
        let start_time = std::time::Instant::now();
        let timeout = Duration::from_secs(30); // Maximum wait time
        let poll_interval = Duration::from_millis(100); // Check every 100ms

        loop {
            let current_size = self.get_log_size().await?;
            
            if current_size == expected_size {
                println!("✅ Log reached expected size: {} (took {:?})", current_size, start_time.elapsed());
                return Ok(());
            }
            
            if current_size > expected_size {
                return Err(anyhow::anyhow!(
                    "❌ Log size mismatch! Expected exactly {}, but got {} (extra {} entries from unknown source). This indicates entries are being processed that weren't tracked by this test.",
                    expected_size,
                    current_size,
                    current_size - expected_size
                ));
            }

            if start_time.elapsed() > timeout {
                return Err(anyhow::anyhow!(
                    "Timeout waiting for log size. Expected {}, got {} after {:?}",
                    expected_size,
                    current_size,
                    timeout
                ));
            }

            tokio::time::sleep(poll_interval).await;
        }
    }

    /// Creates a new test client that interacts with both the HTTP API and database
    /// Default log is "test_log_single_source"
    pub async fn new(api_base_url: &str) -> Result<Self> {
        Self::new_with_log(api_base_url, "test_log_single_source").await
    }

    /// Creates a new test client with a specific log name
    pub async fn new_with_log(api_base_url: &str, log_name: &str) -> Result<Self> {
        // Set up HTTP client with reasonable timeouts
        let http_client = HttpClient::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        // Set up database connection
        let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file");
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
            default_log_name: log_name.to_string(),
        })
    }

    /// Gets the current merkle root
    pub async fn get_root(&self) -> Result<Vec<u8>> {
        let response = self
            .http_client
            .get(&format!("{}/logs/{}/root", self.api_base_url, self.default_log_name))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        // Check for error response
        if response["status"].as_str() == Some("error") {
            return Err(anyhow::anyhow!(
                "Error getting proof: {}",
                response["error"].as_str().unwrap_or("unknown error")
            ));
        }

        // Parse the base64 encoded root from the response
        let root_b64 = response["merkle_root"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("Invalid root response"))?;

        // Decode base64 back to bytes
        let root_bytes = base64::engine::general_purpose::STANDARD.decode(root_b64)?;

        Ok(root_bytes)
    }

    /// Gets an inclusion proof for a given hash or piece of data
    pub async fn get_inclusion_proof(&self, data: &str) -> Result<InclusionProof> {
        // Compute the hash from the data
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        let hash_result = hasher.finalize();

        let query = crate::service::InclusionQuery {
            hash: hash_result.to_vec(),
        };

        // Make the request with the base64 encoded hash
        let response = self
            .http_client
            .get(&format!("{}/logs/{}/proof", self.api_base_url, self.default_log_name))
            .query(&query)
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

    /// Gets a consistency proof proving that the current tree state is consistent with an older root hash
    pub async fn get_consistency_proof(&self, old_root: Vec<u8>) -> Result<ConsistencyProof> {
        let query = crate::service::ConsistencyQuery { old_root };

        let response = self
            .http_client
            .get(&format!("{}/logs/{}/consistency", self.api_base_url, self.default_log_name))
            .query(&query)
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
    pub async fn verify_tree_consistency(&self, old_root: Vec<u8>) -> Result<bool> {
        // Get consistency proof between the old root and current state
        let proof = self.get_consistency_proof(old_root).await?;

        // Get current root to validate the proof matches current state
        let current_root = self.get_root().await?;
        if proof.new_root != current_root {
            return Ok(false);
        }

        // Verify the proof cryptographically
        proof
            .verify()
            .map_err(|e| anyhow::anyhow!("Proof verification failed: {}", e))
    }

    /// Rebuilds the merkle tree from scratch
    pub async fn trigger_rebuild(&self) -> Result<()> {
        let response = self
            .http_client
            .get(&format!("{}/logs/{}/rebuild", self.api_base_url, self.default_log_name))
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

    /// Utilities for interacting with the database directly for testing
    
    /// Adds a new entry to the log through the database
    pub async fn add_entry(&self, data: &str) -> Result<i64> {
        // First compute the hash
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        let hash_result = hasher.finalize();

        // Insert into database
        let row = self
            .db_client
            .query_one(
                "INSERT INTO source_log (data, leaf_hash) VALUES ($1, $2) RETURNING id",
                &[&data, &hash_result.as_slice()],
            )
            .await?;

        Ok(row.get(0))
    }

    /// Idempotently sets up test environment with logs and sources
    /// This can be called multiple times safely - does nothing if already configured
    pub async fn setup_test_environment(&self) -> Result<()> {
        // Define our two test logs
        let logs = vec![
            ("test_log_single_source", "Log for single-source tests", vec!["source_log"]),
            ("test_log_multi_source", "Log for multi-source tests", vec!["source_log", "test_source_a", "test_source_b"]),
        ];

        for (log_name, description, source_tables) in logs {
            // Create log if it doesn't exist (idempotent)
            let result = self.db_client
                .execute(
                    "INSERT INTO verification_logs (log_name, description) VALUES ($1, $2) ON CONFLICT (log_name) DO NOTHING",
                    &[&log_name, &description],
                )
                .await?;
            
            if result > 0 {
                println!("✅ Created log '{}'", log_name);
            }
            
            // Create and register source tables
            for table_name in source_tables {
                // Create table if it doesn't exist (idempotent)
                self.db_client
                    .batch_execute(&format!(
                        r#"
                        CREATE TABLE IF NOT EXISTS {} (
                            id          BIGSERIAL PRIMARY KEY,
                            data        TEXT NOT NULL,
                            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            leaf_hash   BYTEA NOT NULL
                        );
                        "#,
                        table_name
                    ))
                    .await?;
                
                // Register in verification_sources (idempotent)
                let result = self.db_client
                    .execute(
                        "INSERT INTO verification_sources (source_table, log_name, hash_column, order_column) 
                         VALUES ($1, $2, $3, $4) 
                         ON CONFLICT (source_table, log_name) DO NOTHING",
                        &[&table_name, &log_name, &"leaf_hash", &"id"],
                    )
                    .await?;
                
                if result > 0 {
                    println!("✅ Registered '{}' in log '{}'", table_name, log_name);
                }
            }
        }
        
        Ok(())
    }

    /// Legacy function for backwards compatibility - now calls idempotent setup
    #[deprecated(note = "Use setup_test_environment() instead")]
    pub async fn setup_and_register_sources(&self, _table_names: &[&str]) -> Result<()> {
        self.setup_test_environment().await
    }

    pub async fn add_entry_to_source(
        &self,
        table_name: &str,
        data: &str,
    ) -> Result<i64> {
        use sha2::{Digest, Sha256};
        
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        let hash_result = hasher.finalize();

        let row = self.db_client
            .query_one(
                &format!(
                    "INSERT INTO {} (data, leaf_hash) VALUES ($1, $2) RETURNING id",
                    table_name
                ),
                &[&data, &hash_result.as_slice()],
            )
            .await?;

        Ok(row.get(0))
    }

    pub async fn get_sources(&self) -> Result<Vec<tokio_postgres::Row>> {
        let result = self.db_client
        .query(
            "SELECT source_table, source_id, id FROM merkle_log WHERE log_name = $1 ORDER BY id",
            &[&self.default_log_name],
        )
        .await?;

        Ok(result)
    }

    /// Diagnostic: Show recent entries for debugging
    pub async fn show_recent_entries(&self, count: i64) -> Result<()> {
        let rows = self.db_client
            .query(
                "SELECT id, log_name, source_table, source_id FROM merkle_log ORDER BY id DESC LIMIT $1",
                &[&count],
            )
            .await?;

        println!("\n🔍 Last {} entries in merkle_log:", count);
        for row in rows.iter().rev() {
            let id: i64 = row.get(0);
            let log_name: String = row.get(1);
            let source_table: String = row.get(2);
            let source_id: i64 = row.get(3);
            println!("  id={}, log={}, source={}:{}", id, log_name, source_table, source_id);
        }
        println!();
        Ok(())
    }
}
