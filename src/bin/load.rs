//! Load generator for testing Trellis under realistic conditions
//!
//! Simulates a high-throughput application by continuously inserting entries
//! into a test table that Trellis monitors. Uses direct database access.
//! Optionally generates HTTP load by requesting and verifying proofs.
//!
//! # Usage
//!
//! ```bash
//! # Basic load generation (batch processor only)
//! cargo run --bin load -- --rows-per-interval 1000 --interval-secs 1
//!
//! # With HTTP load testing
//! cargo run --bin load -- --http-load --sample-rate 0.01
//! ```
//!
//! # HTTP Load Testing Notes
//!
//! ## Latency Measurement
//!
//! The reported "avg latency" measures wall-clock time from database insert to
//! successful proof verification. This includes polling delay: the HTTP load task
//! sleeps for `--http-interval-ms` between cycles, so in the worst case (leaf
//! becomes verifiable right after we start sleeping), the latency is inflated by
//! nearly the full interval. On average, expect ~`interval/2` of polling overhead.
//!
//! For more accurate latency measurements, use a shorter `--http-interval-ms` at
//! the cost of increased HTTP request volume.
//!
//! ## Consistency Proof Root History
//!
//! Roots are stored in a ring buffer bounded by `--max-roots` (default 100) per log.
//! Once at capacity, the oldest root is evicted. This means consistency proofs can
//! only be tested against roots within roughly `max_roots × http_interval_ms` of
//! history (default: ~50 seconds). Older roots are not retained.
//!
#![allow(clippy::pedantic)]
#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]
#![allow(clippy::arithmetic_side_effects)]

use anyhow::Result;
use chrono::Utc;
use clap::Parser;
use rand::Rng;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tokio_postgres::NoTls;
use trellis::service::Client;

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

    /// Enable HTTP load testing (proof requests and verification)
    #[arg(long, default_value_t = false)]
    http_load: bool,

    /// Fraction of inserted leaves to sample for HTTP testing (0.0-1.0)
    #[arg(long, default_value_t = 0.01)]
    sample_rate: f64,

    /// Maximum number of historical roots to keep per log for consistency proofs
    #[arg(long, default_value_t = 100)]
    max_roots: usize,

    /// Interval between HTTP load cycles in milliseconds
    #[arg(long, default_value_t = 500)]
    http_interval_ms: u64,
}

/// A leaf waiting to be verified via HTTP
struct PendingLeaf {
    /// The leaf hash
    hash: Vec<u8>,
    /// The log this leaf belongs to
    log_name: String,
    /// When the leaf was inserted into the source table
    insert_time: Instant,
}

/// State for HTTP load generation
struct HttpLoadState {
    /// Leaves waiting to be verified
    pending: VecDeque<PendingLeaf>,
    /// Historical roots per log: (root_hash, tree_size)
    roots: HashMap<String, VecDeque<(Vec<u8>, u64)>>,
    /// Maximum roots to keep per log
    max_roots: usize,
    /// Statistics
    stats: HttpLoadStats,
}

/// Statistics for HTTP load testing
#[derive(Default)]
struct HttpLoadStats {
    /// Number of has_leaf requests made
    has_leaf_requests: u64,
    /// Number of inclusion proofs successfully verified
    inclusion_proofs_verified: u64,
    /// Number of consistency proofs successfully verified
    consistency_proofs_verified: u64,
    /// Total latency from insert to verification (ms)
    total_latency_ms: u64,
    /// Number of errors encountered
    errors: u64,
}

impl HttpLoadState {
    /// Create a new HTTP load state with the given maximum root history size
    fn new(max_roots: usize) -> Self {
        Self {
            pending: VecDeque::new(),
            roots: HashMap::new(),
            max_roots,
            stats: HttpLoadStats::default(),
        }
    }

    /// Add a sampled leaf to the pending queue
    fn add_pending(&mut self, hash: Vec<u8>, log_name: String) {
        self.pending.push_back(PendingLeaf {
            hash,
            log_name,
            insert_time: Instant::now(),
        });
    }

    /// Add a root to the history for a log
    fn add_root(&mut self, log_name: &str, root: Vec<u8>, tree_size: u64) {
        let roots = self
            .roots
            .entry(log_name.to_string())
            .or_insert_with(|| VecDeque::with_capacity(self.max_roots));

        if roots.len() >= self.max_roots {
            roots.pop_front();
        }
        roots.push_back((root, tree_size));
    }

    /// Get a random historical root for consistency proof testing
    fn get_random_old_root(&self, log_name: &str) -> Option<(Vec<u8>, u64)> {
        let roots = self.roots.get(log_name)?;
        if roots.len() < 2 {
            return None; // Need at least 2 roots to pick an old one
        }
        // Pick a random root that's not the latest
        let idx = rand::rng().random_range(0..roots.len() - 1);
        roots.get(idx).cloned()
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    let args = Args::parse();

    // Configuration from command line arguments
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let server_url =
        std::env::var("TRELLIS_SERVER_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());

    println!("=== Trellis Load Generator ===");
    println!("Rows per interval: {}", args.rows_per_interval);
    println!("Interval: {}s", args.interval_secs);
    println!("Number of logs: {}", args.num_logs);
    println!("Sources per log: {}", args.num_sources);
    if args.http_load {
        println!("HTTP load: enabled (sample rate: {:.1}%)", args.sample_rate * 100.0);
        println!("HTTP interval: {}ms", args.http_interval_ms);
        println!("Server: {}", server_url);
    }
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

    // Initialize HTTP load state if enabled
    let http_state = if args.http_load {
        Some(Arc::new(Mutex::new(HttpLoadState::new(args.max_roots))))
    } else {
        None
    };

    // Spawn HTTP load task if enabled
    if let Some(state) = http_state.clone() {
        let http_client = Client::new(&server_url)?;
        let http_interval = Duration::from_millis(args.http_interval_ms);
        let log_names: Vec<String> = (0..args.num_logs)
            .map(|i| format!("load_test_{i}"))
            .collect();

        tokio::spawn(async move {
            http_load_task(http_client, state, http_interval, log_names).await;
        });
    }

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

                let (inserted, sampled_hashes) =
                    insert_rows(&client, &source_table, rows_per_source, &mut counter, args.sample_rate).await?;

                total_inserted += inserted;

                // Add sampled hashes to pending queue for HTTP load testing
                if let Some(state) = &http_state {
                    let mut state = state.lock().await;
                    for hash in sampled_hashes {
                        state.add_pending(hash, log_name.clone());
                    }
                }
            }

            print!(
                "  {} -> {} rows",
                log_name, total_inserted
            );
            
            // Show HTTP stats if enabled
            if let Some(state) = &http_state {
                let state = state.lock().await;
                let pending = state.pending.len();
                print!(" | pending: {}", pending);
            }
            println!();
        }

        let elapsed = start.elapsed();
        
        // Print summary line
        print!("Batch complete in {elapsed:?} | Total rows: {counter}");
        if let Some(state) = &http_state {
            let state = state.lock().await;
            let stats = &state.stats;
            let avg_latency = if stats.inclusion_proofs_verified > 0 {
                stats.total_latency_ms / stats.inclusion_proofs_verified
            } else {
                0
            };
            print!(
                " | HTTP: {} inc (avg {}ms), {} con verified | {} errors",
                stats.inclusion_proofs_verified,
                avg_latency,
                stats.consistency_proofs_verified,
                stats.errors
            );
        }
        println!("\n");

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
/// Returns (rows_inserted, sampled_hashes)
async fn insert_rows(
    client: &tokio_postgres::Client,
    table_name: &str,
    count: u32,
    counter: &mut u64,
    sample_rate: f64,
) -> Result<(u32, Vec<Vec<u8>>)> {
    if count == 0 {
        return Ok((0, Vec::new()));
    }

    // PostgreSQL has a limit of ~65535 parameters per query
    // With 3 columns per row, we can safely do ~21000 rows per query
    // Use 1000 as a conservative chunk size
    const CHUNK_SIZE: u32 = 1000;

    let mut total_inserted = 0;
    let mut remaining = count;
    let mut sampled_hashes = Vec::new();

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
            
            // Sample this hash for HTTP load testing
            if sample_rate > 0.0 && rand::rng().random::<f64>() < sample_rate {
                sampled_hashes.push(hash.clone());
            }
            
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

    Ok((total_inserted, sampled_hashes))
}

/// Compute SHA-256 hash of data
fn compute_hash(data: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(data.as_bytes());
    hasher.finalize().to_vec()
}

/// Background task that generates HTTP load by verifying proofs
async fn http_load_task(
    client: Client,
    state: Arc<Mutex<HttpLoadState>>,
    interval: Duration,
    log_names: Vec<String>,
) {
    loop {
        // Process pending leaves (check has_leaf, request proofs)
        process_pending_leaves(&client, &state).await;

        // Sample roots for consistency proof testing
        sample_roots(&client, &state, &log_names).await;

        // Test consistency proofs
        test_consistency_proofs(&client, &state, &log_names).await;

        tokio::time::sleep(interval).await;
    }
}

/// Process pending leaves: check has_leaf, request and verify inclusion proofs
async fn process_pending_leaves(client: &Client, state: &Arc<Mutex<HttpLoadState>>) {
    // Take all pending leaves to process
    let to_process: Vec<PendingLeaf> = {
        let mut state = state.lock().await;
        state.pending.drain(..).collect()
    };

    let mut still_pending = Vec::new();

    for leaf in to_process {
        // Increment has_leaf request counter
        {
            let mut state = state.lock().await;
            state.stats.has_leaf_requests += 1;
        }

        // Check if leaf exists yet
        match client.has_leaf_hash(&leaf.log_name, &leaf.hash).await {
            Ok(exists) => {
                if !exists {
                    // Not yet processed, put back in queue
                    still_pending.push(leaf);
                    continue;
                }

                // Leaf exists, request and verify inclusion proof
                match client.get_inclusion_proof(&leaf.log_name, &leaf.hash).await {
                    Ok(proof) => {
                        // Verify the proof
                        match client.verify_inclusion_proof(&leaf.hash, &proof) {
                            Ok(()) => {
                                let latency_ms = leaf.insert_time.elapsed().as_millis() as u64;
                                let mut state = state.lock().await;
                                state.stats.inclusion_proofs_verified += 1;
                                state.stats.total_latency_ms += latency_ms;
                            }
                            Err(e) => {
                                let mut state = state.lock().await;
                                state.stats.errors += 1;
                                eprintln!("❌ Inclusion proof verification failed for log {}: {e}", leaf.log_name);
                            }
                        }
                    }
                    Err(e) => {
                        let mut state = state.lock().await;
                        state.stats.errors += 1;
                        eprintln!("❌ Failed to get inclusion proof: {e}");
                    }
                }
            }
            Err(e) => {
                let mut state = state.lock().await;
                state.stats.errors += 1;
                eprintln!("❌ has_leaf request failed: {e}");
            }
        }
    }

    // Put still-pending leaves back
    if !still_pending.is_empty() {
        let mut state = state.lock().await;
        for leaf in still_pending {
            state.pending.push_front(leaf); // Put at front to retry sooner
        }
    }
}

/// Sample current roots from all logs for consistency proof testing
async fn sample_roots(client: &Client, state: &Arc<Mutex<HttpLoadState>>, log_names: &[String]) {
    for log_name in log_names {
        match client.get_root(log_name).await {
            Ok(root_response) => {
                let mut state = state.lock().await;
                state.add_root(log_name, root_response.root, root_response.tree_size);
            }
            Err(_) => {
                // Ignore errors - tree might be empty
            }
        }
    }
}

/// Test consistency proofs between historical roots and current state
async fn test_consistency_proofs(
    client: &Client,
    state: &Arc<Mutex<HttpLoadState>>,
    log_names: &[String],
) {
    for log_name in log_names {
        // Get a random old root for this log
        let old_root = {
            let state = state.lock().await;
            state.get_random_old_root(log_name)
        };

        let Some((old_root_hash, _old_size)) = old_root else {
            continue; // Not enough roots yet
        };

        // Request consistency proof
        match client.get_consistency_proof(log_name, old_root_hash.clone()).await {
            Ok(proof) => {
                // Verify the proof
                match client.verify_consistency_proof(&old_root_hash, &proof) {
                    Ok(()) => {
                        let mut state = state.lock().await;
                        state.stats.consistency_proofs_verified += 1;
                    }
                    Err(e) => {
                        let mut state = state.lock().await;
                        state.stats.errors += 1;
                        eprintln!("❌ Consistency proof verification failed for log {log_name}: {e}");
                    }
                }
            }
            Err(e) => {
                let mut state = state.lock().await;
                state.stats.errors += 1;
                eprintln!("❌ Failed to get consistency proof for log {log_name}: {e}");
            }
        }
    }
}
