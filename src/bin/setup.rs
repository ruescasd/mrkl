/// cargo run --bin setup -- --reset
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
            DROP TABLE IF EXISTS merkle_log;
        "#,
            )
            .await?;
        println!("✅ Existing objects dropped.");
    }

    // 1. Create the verification sources configuration table
    client
        .batch_execute(
            r#"
        CREATE TABLE IF NOT EXISTS verification_sources (
            id SERIAL PRIMARY KEY,
            source_table TEXT NOT NULL,
            hash_column TEXT NOT NULL,
            order_column TEXT NOT NULL,
            enabled BOOLEAN NOT NULL DEFAULT true,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            UNIQUE(source_table)
        );
    "#,
        )
        .await?;
    println!("✅ Table 'verification_sources' is ready.");

    // 2. Create the merkle log table with strict controls - supports multiple sources
    client
        .batch_execute(
            r#"
        CREATE TABLE IF NOT EXISTS merkle_log (
            id BIGINT PRIMARY KEY,
            source_table TEXT NOT NULL,
            source_id BIGINT NOT NULL,
            leaf_hash BYTEA NOT NULL,
            processed_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            CONSTRAINT merkle_log_immutable CHECK (processed_at = processed_at)
        );

        -- Index on (source_table, source_id) for lookups and duplicate prevention across sources
        CREATE UNIQUE INDEX merkle_log_source_idx ON merkle_log(source_table, source_id);
        -- Index on leaf_hash for proof generation
        CREATE INDEX merkle_log_hash_idx ON merkle_log(leaf_hash);
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
