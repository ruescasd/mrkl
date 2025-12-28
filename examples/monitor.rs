//! Example: Consistency Monitoring (CT-style Auditor)
//!
//! This example demonstrates how to continuously monitor a merkle log for consistency.
//! It periodically checks if the log has grown, and when it has, it verifies that the
//! new root is consistent with the previous root using a consistency proof.
//!
//! This is similar to how Certificate Transparency monitors work - they ensure that
//! logs are append-only and detect any attempts to rewrite history.
//!
//! Usage:
//!   cargo run --example monitor -- `<log_name>`
//!
//! Example:
//!   cargo run --example monitor -- `example_post_log`
#![allow(clippy::pedantic)]
#![allow(clippy::print_stdout)]
#![allow(clippy::print_stderr)]
#![allow(clippy::arithmetic_side_effects)]

use anyhow::Result;
use base64::Engine;
use mrkl::service::Client;
use std::time::Duration;

/// Represents a snapshot of a merkle log's state at a point in time.
#[derive(Debug)]
struct LogState {
    /// The size of the merkle tree (number of leaves).
    size: u64,
    /// The root hash of the merkle tree.
    root: Vec<u8>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    println!("=== MRKL Consistency Monitor ===\n");

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        let program_name = args.first().map(String::as_str).unwrap_or("monitor");
        eprintln!("Usage: {program_name} <log_name>");
        eprintln!("\nExample: {program_name} example_post_log");
        std::process::exit(1);
    }

    let log_name = args.get(1).expect("log_name argument required");

    // Configuration
    let server_url =
        std::env::var("MRKL_SERVER_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());
    let poll_interval_secs: u64 = std::env::var("MONITOR_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(5);

    println!("📡 Monitoring log: {log_name}");
    println!("🔗 Server: {server_url}");
    println!("⏱️  Poll interval: {poll_interval_secs}s");
    println!();

    // Create client
    let client = Client::new(&server_url)?;

    // Get initial state
    println!("🔍 Fetching initial state...");
    let mut state = match fetch_log_state(&client, log_name).await {
        Ok(s) => {
            println!("✅ Initial state:");
            print_state(&s);
            println!();
            s
        }
        Err(e) => {
            eprintln!("❌ Failed to fetch initial state: {e}");
            eprintln!("   Make sure the log exists and the server is running.");
            std::process::exit(1);
        }
    };

    println!("👁️  Monitoring for changes... (Ctrl+C to stop)\n");

    let mut check_count = 0;
    loop {
        tokio::time::sleep(Duration::from_secs(poll_interval_secs)).await;
        check_count += 1;

        match fetch_log_state(&client, log_name).await {
            Ok(new_state) => {
                if new_state.size != state.size {
                    println!(
                        "📊 [Check #{}] Size changed: {} → {}",
                        check_count, state.size, new_state.size
                    );

                    // Verify consistency
                    match verify_consistency(&client, log_name, &state, &new_state).await {
                        Ok(true) => {
                            println!("   ✅ Consistency proof VERIFIED");
                            println!(
                                "   → Log correctly appended {} new entries",
                                new_state.size - state.size
                            );
                        }
                        Ok(false) => {
                            println!("   ❌ CONSISTENCY VERIFICATION FAILED!");
                        }
                        Err(e) => {
                            println!("   ⚠️  Error verifying consistency: {e}");
                        }
                    }

                    print_state(&new_state);
                    println!();

                    // Update our tracked state
                    state = new_state;
                } else if new_state.root != state.root {
                    println!(
                        "⚠️  [Check #{check_count}] Root changed but size unchanged! (Possible issue)"
                    );
                    print_state(&new_state);
                    println!();
                    state = new_state;
                } else {
                    // No change - print periodic status
                    println!(
                        "💤 [Check #{}] No changes (size: {})",
                        check_count, state.size
                    );
                }
            }
            Err(e) => {
                println!("⚠️  [Check #{check_count}] Failed to fetch state: {e}");
            }
        }
    }
}

/// Fetches the current state (size and root) of a log
async fn fetch_log_state(client: &Client, log_name: &str) -> Result<LogState> {
    let size = client.get_log_size(log_name).await?;
    let root = if size > 0 {
        client.get_root(log_name).await?
    } else {
        Vec::new()
    };

    Ok(LogState { size, root })
}

/// Verifies that the new state is consistent with the old state
async fn verify_consistency(
    client: &Client,
    log_name: &str,
    old_state: &LogState,
    new_state: &LogState,
) -> Result<bool> {
    if old_state.size == 0 {
        // No previous state to verify against
        return Ok(true);
    }

    if new_state.size < old_state.size {
        // Log size decreased - this should never happen!
        println!("   ⚠️  WARNING: Tree size decreased!");
        return Ok(false);
    }

    if new_state.size == old_state.size {
        // Same size, roots should match
        return Ok(new_state.root == old_state.root);
    }

    // Request consistency proof
    let is_consistent = client
        .verify_tree_consistency(log_name, old_state.root.clone())
        .await?;

    Ok(is_consistent)
}

/// Prints a formatted representation of log state
fn print_state(state: &LogState) {
    println!("   Size: {}", state.size);
    if !state.root.is_empty() {
        let root_b64 = base64::engine::general_purpose::STANDARD.encode(&state.root);
        println!("   Root: {}...", &root_b64[..min(16, root_b64.len())]);
    } else {
        println!("   Root: (empty)");
    }
}

/// Returns the minimum of two values.
const fn min(a: usize, b: usize) -> usize {
    if a < b { a } else { b }
}
