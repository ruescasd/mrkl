//! Load generator for testing Trellis under realistic conditions
//!
//! Simulates a high-throughput application by continuously inserting entries
//! into a test table that Trellis monitors. Uses direct database access.
//!
#![allow(clippy::pedantic)]
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

/// Load generator for testing Trellis under realistic conditions
#[derive(Parser, Debug)]
#[command(name = "load")]
#[command(about = "Generate continuous load for Trellis testing", long_about = None)]
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

    println!("=== Trellis Load Generator ===");
    println!("Rows per interval: {}", args.rows_per_interval);
    println!("Interval: {}s", args.interval_secs);
    println!("Number of logs: {}", args.num_logs);
    println!("Sources per log: {}", args.num_sources);
    println!();

    // Connect to database
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            println!("Connection error: {e}");
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
            let log_name = format!("load_test_{log_idx}");

            let mut total_inserted = 0;

            // Insert into all source tables for this log
            for source_idx in 0..args.num_sources {
                let source_table = format!("{log_name}_source_{source_idx}");
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
            "Batch complete in {elapsed:?} | Total rows: {counter}\n"
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
        let log_name = format!("load_test_{log_idx}");

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
            let source_table = format!("{log_name}_source_{source_idx}");

            // Create source table (all have same schema with timestamp)
            client
                .execute(
                    &format!(
                        "CREATE TABLE IF NOT EXISTS {source_table} (
                            id BIGSERIAL PRIMARY KEY,
                            data TEXT NOT NULL,
                            hash BYTEA NOT NULL,
                            created_at TIMESTAMPTZ DEFAULT NOW()
                        )"
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

        println!("  ✓ Configured log: {log_name} ({num_sources} sources)");
    }

    Ok(())
}

/// Insert rows into a source table using multi-row INSERT for performance
async fn insert_rows(
    client: &tokio_postgres::Client,
    table_name: &str,
    count: u32,
    counter: &mut u64,
) -> Result<u32> {
    if count == 0 {
        return Ok(0);
    }

    // PostgreSQL has a limit of ~65535 parameters per query
    // With 3 columns per row, we can safely do ~21000 rows per query
    // Use 1000 as a conservative chunk size
    const CHUNK_SIZE: u32 = 1000;

    let mut total_inserted = 0;
    let mut remaining = count;

    while remaining > 0 {
        let batch_size = remaining.min(CHUNK_SIZE);
        
        // Prepare data for this batch
        let mut values_clauses = Vec::with_capacity(batch_size as usize);
        let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::with_capacity((batch_size * 3) as usize);
        
        // Build data vectors
        let mut data_vec = Vec::with_capacity(batch_size as usize);
        let mut hash_vec = Vec::with_capacity(batch_size as usize);
        let mut timestamp_vec = Vec::with_capacity(batch_size as usize);
        
        for i in 0..batch_size {
            *counter += 1;
            let data = format!("entry_{counter}");
            let hash = compute_hash(&data);
            let timestamp = Utc::now();
            
            data_vec.push(data);
            hash_vec.push(hash);
            timestamp_vec.push(timestamp);
            
            // Build VALUES clause: ($1, $2, $3), ($4, $5, $6), ...
            let param_offset = (i * 3) + 1;
            values_clauses.push(format!(
                "(${}, ${}, ${})",
                param_offset,
                param_offset + 1,
                param_offset + 2
            ));
        }
        
        // Build params vector (need references with correct lifetime)
        for i in 0..batch_size as usize {
            params.push(data_vec.get(i).expect("data_vec.len() == batch_size"));
            params.push(hash_vec.get(i).expect("hash_vec.len() == batch_size"));
            params.push(timestamp_vec.get(i).expect("timestamp_vec.len() == batch_size"));
        }
        
        // Construct multi-row INSERT query
        let query = format!(
            "INSERT INTO {} (data, hash, created_at) VALUES {}",
            table_name,
            values_clauses.join(", ")
        );
        
        // Execute the batch insert
        client.execute(&query, &params).await?;
        
        total_inserted += batch_size;
        remaining -= batch_size;
    }

    Ok(total_inserted)
}

/// Compute SHA-256 hash of data
fn compute_hash(data: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data.as_bytes());
    hasher.finalize().to_vec()
}
