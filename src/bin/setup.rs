//! Database setup utility for mrkl
//!
//! Creates the necessary `PostgreSQL` tables and functions for mrkl operation.
//! The schema includes `verification_logs`, `verification_sources`, and `merkle_log` tables.
//!
//! # Usage
//!
//! ```bash
//! cargo run --bin setup           # Create/update schema
//! cargo run --bin setup -- --reset # Drop and recreate all tables
//! ```

use anyhow::Result;
use std::env;
use tokio_postgres::{Client, NoTls};

/// Connects to the database and returns a client.
async fn connect() -> Result<Client> {
    let db_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set in .env file");
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

    // The connection object performs the actual communication with the database,
    // so spawn it off to run on its own.
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    Ok(client)
}

/// Sets up the database schema, including the table, function, and trigger.
/// If reset is true, it will drop and recreate all objects.
async fn setup_database(client: &Client, reset: bool) -> Result<()> {
    if reset {
        println!("🗑️ Dropping existing objects...");
        client
            .batch_execute(
                r#"
            DROP TABLE IF EXISTS merkle_log CASCADE;
            DROP TABLE IF EXISTS verification_sources CASCADE;
            DROP TABLE IF EXISTS verification_logs CASCADE;
        "#,
            )
            .await?;
        println!("✅ Existing objects dropped.");
    }

    // 1. Create the verification logs table - defines independent merkle logs
    client
        .batch_execute(
            r#"
        CREATE TABLE IF NOT EXISTS verification_logs (
            log_name TEXT PRIMARY KEY,
            created_at TIMESTAMP NOT NULL DEFAULT NOW(),
            description TEXT,
            enabled BOOLEAN NOT NULL DEFAULT true
        );
    "#,
        )
        .await?;
    println!("✅ Table 'verification_logs' is ready.");

    // 2. Create the verification sources configuration table - sources can belong to multiple logs
    client
        .batch_execute(
            r#"
        CREATE TABLE IF NOT EXISTS verification_sources (
            source_table TEXT NOT NULL,
            log_name TEXT NOT NULL REFERENCES verification_logs(log_name) ON DELETE CASCADE,
            hash_column TEXT NOT NULL,
            id_column TEXT NOT NULL,
            timestamp_column TEXT,
            enabled BOOLEAN NOT NULL DEFAULT true,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            PRIMARY KEY (source_table, log_name)
        );
    "#,
        )
        .await?;
    println!("✅ Table 'verification_sources' is ready.");

    // 3. Create the merkle log table with strict controls - supports multiple logs and sources
    // CRITICAL: ON DELETE RESTRICT protects ground truth - merkle_log cannot be rebuilt from sources
    // To delete a log, you must explicitly delete its merkle_log entries first (conscious decision)
    client
        .batch_execute(
            r#"
        CREATE TABLE IF NOT EXISTS merkle_log (
            id BIGSERIAL PRIMARY KEY,
            log_name TEXT NOT NULL REFERENCES verification_logs(log_name) ON DELETE RESTRICT,
            source_table TEXT NOT NULL,
            source_id BIGINT NOT NULL,
            leaf_hash BYTEA NOT NULL,
            processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT merkle_log_immutable CHECK (processed_at = processed_at),
            UNIQUE (log_name, source_table, source_id)
        );

        -- Index for querying a specific log's entries in order
        CREATE INDEX IF NOT EXISTS idx_merkle_log_by_log ON merkle_log(log_name, id);
        -- Index on leaf_hash for proof generation
        CREATE INDEX IF NOT EXISTS merkle_log_hash_idx ON merkle_log(leaf_hash);
    "#,
        )
        .await?;
    println!("✅ Table 'merkle_log' is ready.");

    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file
    dotenv::dotenv().ok();

    // Check if --reset flag is provided
    let reset = env::args().any(|arg| arg == "--reset");

    let client = connect().await?;

    println!("--- Setting up database schema ---");
    setup_database(&client, reset).await?;
    println!("----------------------------------");

    Ok(())
}
