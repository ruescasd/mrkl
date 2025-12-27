//! Load generator for testing mrkl under realistic conditions
//!
//! Simulates a high-throughput application by continuously inserting entries
//! into a test table that mrkl monitors. Uses direct database access.
//!
#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]
#![allow(clippy::arithmetic_side_effects)]
//! # Usage
//!
//! ```bash
//! cargo run --bin load -- --entries 1000 --batch-size 100
//! ```

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use sha2::{Digest, Sha256};
use std::time::Duration;
use tokio_postgres::NoTls;

/// Load generator for testing mrkl under realistic conditions
#[derive(Parser, Debug)]
#[command(name = "load")]
#[command(about = "Generate continuous load for mrkl testing", long_about = None)]
struct Args {
    /// Number of rows to insert per interval
    #[arg(short, long, default_value_t = 100)]
    rows_per_interval: u32,

    /// Interval between batches in seconds
    #[arg(short, long, default_value_t = 1)]
    interval_secs: u64,

    /// Number of logs to create and populate
    #[arg(short, long, default_value_t = 3)]
    num_logs: usize,

    /// Number of source tables per log
    #[arg(short = 's', long, default_value_t = 2)]
    num_sources: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let args = Args::parse();

    // Configuration from command line arguments
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    println!("=== MRKL Load Generator ===");
    println!("Rows per interval: {}", args.rows_per_interval);
    println!("Interval: {}s", args.interval_secs);
    println!("Number of logs: {}", args.num_logs);
    println!("Sources per log: {}", args.num_sources);
    println!();

    // Connect to database
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    println!("🔄 Setting up test environment...");
    setup_test_environment(&client, args.num_logs, args.num_sources).await?;
    println!("✅ Setup complete\n");

    println!("📊 Starting load generation (Ctrl+C to stop)...\n");

    let mut counter = 0u64;
    let interval = Duration::from_secs(args.interval_secs);

    loop {
        let start = std::time::Instant::now();

        // Insert rows into all logs and all sources
        for log_idx in 0..args.num_logs {
            let log_name = format!("load_test_{}", log_idx);

            let mut total_inserted = 0;

            // Insert into all source tables for this log
            for source_idx in 0..args.num_sources {
                let source_table = format!("{}_source_{}", log_name, source_idx);
                let rows_per_source = args.rows_per_interval / args.num_sources as u32;

                let inserted =
                    insert_rows(&client, &source_table, rows_per_source, &mut counter).await?;

                total_inserted += inserted;
            }

            println!(
                "  {} -> {} rows across {} sources",
                log_name, total_inserted, args.num_sources
            );
        }

        let elapsed = start.elapsed();
        println!(
            "Batch complete in {:?} | Total rows: {}\n",
            elapsed, counter
        );

        // Wait for next interval
        if elapsed < interval {
            tokio::time::sleep(interval - elapsed).await;
        }
    }
}

/// Idempotently set up test logs, source tables, and configurations
async fn setup_test_environment(
    client: &tokio_postgres::Client,
    num_logs: usize,
    num_sources: usize,
) -> Result<()> {
    for log_idx in 0..num_logs {
        let log_name = format!("load_test_{}", log_idx);

        // Create log entry
        client
            .execute(
                "INSERT INTO verification_logs (log_name, enabled) 
                 VALUES ($1, true) 
                 ON CONFLICT (log_name) DO UPDATE SET enabled = true",
                &[&log_name],
            )
            .await?;

        // Create N source tables for this log
        for source_idx in 0..num_sources {
            let source_table = format!("{}_source_{}", log_name, source_idx);

            // Create source table (all have same schema with timestamp)
            client
                .execute(
                    &format!(
                        "CREATE TABLE IF NOT EXISTS {} (
                            id BIGSERIAL PRIMARY KEY,
                            data TEXT NOT NULL,
                            hash BYTEA NOT NULL,
                            created_at TIMESTAMPTZ DEFAULT NOW()
                        )",
                        source_table
                    ),
                    &[],
                )
                .await?;

            // Configure as source for the log
            client
                .execute(
                    "INSERT INTO verification_sources 
                     (log_name, source_table, hash_column, id_column, timestamp_column, enabled)
                     VALUES ($1, $2, 'hash', 'id', 'created_at', true)
                     ON CONFLICT (log_name, source_table) 
                     DO UPDATE SET enabled = true, timestamp_column = 'created_at'",
                    &[&log_name, &source_table],
                )
                .await?;
        }

        println!("  ✓ Configured log: {} ({} sources)", log_name, num_sources);
    }

    Ok(())
}

/// Insert rows into a source table
async fn insert_rows(
    client: &tokio_postgres::Client,
    table_name: &str,
    count: u32,
    counter: &mut u64,
) -> Result<u32> {
    for _ in 0..count {
        *counter += 1;
        let data = format!("entry_{}", counter);
        let hash = compute_hash(&data);

        client
            .execute(
                &format!(
                    "INSERT INTO {} (data, hash, created_at) VALUES ($1, $2, $3)",
                    table_name
                ),
                &[&data, &hash, &Utc::now()],
            )
            .await?;
    }

    Ok(count)
}

/// Compute SHA-256 hash of data
fn compute_hash(data: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data.as_bytes());
    hasher.finalize().to_vec()
}
