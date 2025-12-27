//! Integration tests for the mrkl HTTP client
//!
//! Tests the Client API for interacting with the mrkl server, including
//! proof requests, verification, and error handling.

use anyhow::Result;
use mrkl::service::Client;
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio_postgres::NoTls;

/// Test client that wraps the production HTTP client and adds database utilities
pub struct TestClient {
    /// Production HTTP client
    pub client: Client,
    /// Direct database connection for test setup and assertions
    pub db_client: tokio_postgres::Client,
}

impl TestClient {
    /// Creates a new test client with both HTTP and database access
    pub async fn new(api_base_url: &str) -> Result<Self> {
        // Create production HTTP client
        let client = Client::new(api_base_url)?;

        // Set up database connection
        let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file");
        let (db_client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(Self { client, db_client })
    }

    // ===== Test Setup Utilities =====

    /// Idempotently sets up test environment with logs and sources
    /// This can be called multiple times safely - does nothing if already configured
    pub async fn setup_test_environment(&self) -> Result<()> {
        // Define our test logs
        // Format: (log_name, description, [(source_table, has_timestamp)])
        #[allow(clippy::type_complexity)]
        let logs: Vec<(&str, &str, Vec<(&str, bool)>)> = vec![
            (
                "test_log_single_source",
                "Log for single-source tests",
                vec![("source_log", true)],
            ),
            (
                "test_log_multi_source",
                "Log for multi-source tests",
                vec![
                    ("source_log", true),
                    ("test_source_a", true),
                    ("test_source_b", true),
                    ("source_no_timestamp", false),
                ],
            ),
            (
                "test_log_no_timestamp",
                "Log for testing sources without timestamps",
                vec![
                    ("source_no_timestamp", false),
                    ("source_no_timestamp_b", false),
                ],
            ),
        ];

        for (log_name, description, source_configs) in logs {
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
            for (table_name, has_timestamp) in source_configs {
                // Create table if it doesn't exist (idempotent)
                let create_table_sql = if has_timestamp {
                    format!(
                        r#"
                        CREATE TABLE IF NOT EXISTS {} (
                            id          BIGSERIAL PRIMARY KEY,
                            data        TEXT NOT NULL,
                            created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                            leaf_hash   BYTEA NOT NULL
                        );
                        "#,
                        table_name
                    )
                } else {
                    format!(
                        r#"
                        CREATE TABLE IF NOT EXISTS {} (
                            id          BIGSERIAL PRIMARY KEY,
                            data        TEXT NOT NULL,
                            leaf_hash   BYTEA NOT NULL
                        );
                        "#,
                        table_name
                    )
                };

                self.db_client.batch_execute(&create_table_sql).await?;

                // Register in verification_sources (idempotent)
                let timestamp_column = if has_timestamp {
                    Some("created_at")
                } else {
                    None
                };
                let result = self.db_client
                    .execute(
                        "INSERT INTO verification_sources (source_table, log_name, hash_column, id_column, timestamp_column) 
                         VALUES ($1, $2, $3, $4, $5) 
                         ON CONFLICT (source_table, log_name) DO NOTHING",
                        &[&table_name, &log_name, &"leaf_hash", &"id", &timestamp_column],
                    )
                    .await?;

                if result > 0 {
                    println!(
                        "✅ Registered '{}' in log '{}' (timestamp: {})",
                        table_name, log_name, has_timestamp
                    );
                }
            }
        }

        Ok(())
    }

    // ===== Database Insertion Utilities =====

    /// Adds a new entry to the source_log table
    pub async fn add_entry(&self, data: &str) -> Result<i64> {
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        let hash_result = hasher.finalize();

        let row = self
            .db_client
            .query_one(
                "INSERT INTO source_log (data, leaf_hash) VALUES ($1, $2) RETURNING id",
                &[&data, &hash_result.as_slice()],
            )
            .await?;

        Ok(row.get(0))
    }

    /// Adds an entry to a specified source table
    pub async fn add_entry_to_source(&self, table_name: &str, data: &str) -> Result<i64> {
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        let hash_result = hasher.finalize();

        let row = self
            .db_client
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

    // ===== Test Assertion Utilities =====

    /// Gets all sources from merkle_log for a given log
    pub async fn get_sources(&self, log_name: &str) -> Result<Vec<tokio_postgres::Row>> {
        let result = self.db_client
            .query(
                "SELECT source_table, source_id, id FROM merkle_log WHERE log_name = $1 ORDER BY id",
                &[&log_name],
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
            println!(
                "  id={}, log={}, source={}:{}",
                id, log_name, source_table, source_id
            );
        }
        println!();
        Ok(())
    }

    // ===== Test Synchronization Utilities =====

    /// Waits until the log reaches the expected size by polling the size endpoint
    /// This is much more efficient than fixed-duration sleeps
    /// STRICT: Returns error if size exceeds expected (indicates entries from elsewhere)
    pub async fn wait_until_log_size(&self, log_name: &str, expected_size: usize) -> Result<()> {
        let start_time = std::time::Instant::now();
        let timeout = Duration::from_secs(30); // Maximum wait time
        let poll_interval = Duration::from_millis(100); // Check every 100ms

        loop {
            let current_size = self.client.get_log_size(log_name).await?;

            if current_size == expected_size {
                println!(
                    "✅ Log reached expected size: {} (took {:?})",
                    current_size,
                    start_time.elapsed()
                );
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
}
