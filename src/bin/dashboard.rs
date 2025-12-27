//! Real-time metrics dashboard for mrkl
//!
//! A terminal-based dashboard that displays live metrics from the mrkl server,
//! including per-log processing statistics and global system metrics.
//!
//! # Usage
//!
//! ```bash
//! cargo run --bin dashboard
//! ```
//!
//! Press Ctrl+C to exit.

use anyhow::Result;
use crossterm::{
    cursor, execute,
    terminal::{self, ClearType},
};
use serde::Deserialize;
use std::collections::HashMap;
use std::io::{Write, stdout};
use std::time::Duration;

#[derive(Debug, Deserialize)]
struct MetricsResponse {
    logs: HashMap<String, LogMetrics>,
    global: GlobalMetrics,
}

#[derive(Debug, Deserialize)]
struct LogMetrics {
    last_batch_rows: u64,
    last_batch_leaves: u64,
    last_total_ms: u64,
    last_copy_ms: u64,
    last_query_sources_ms: u64,
    last_insert_merkle_log_ms: u64,
    last_fetch_merkle_log_ms: u64,
    last_tree_update_ms: u64,
    tree_size: u64,
    tree_memory_bytes: u64,
    last_update: String,
}

#[derive(Debug, Deserialize)]
struct GlobalMetrics {
    last_cycle_duration_ms: u64,
    last_active_log_count: usize,
    last_cycle_fraction: f64,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "status", rename_all = "lowercase")]
enum ApiResponse {
    #[serde(rename = "ok")]
    Ok {
        logs: HashMap<String, LogMetrics>,
        global: GlobalMetrics,
    },
    Error {
        error: String,
    },
}

async fn fetch_metrics(url: &str) -> Result<MetricsResponse> {
    let response = reqwest::get(url).await?;
    let text = response.text().await?;

    let api_response: ApiResponse = serde_json::from_str(&text)?;

    match api_response {
        ApiResponse::Ok { logs, global } => {
            // eprintln!("Parsed {} logs", logs.len());
            Ok(MetricsResponse { logs, global })
        }
        ApiResponse::Error { error } => Err(anyhow::anyhow!("API error: {}", error)),
    }
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000 {
        format!("{:.1}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.1}k", n as f64 / 1_000.0)
    } else {
        n.to_string()
    }
}

fn format_memory(bytes: u64) -> String {
    if bytes >= 1_073_741_824 {
        format!("{:.1}GB", bytes as f64 / 1_073_741_824.0)
    } else if bytes >= 1_048_576 {
        format!("{:.1}MB", bytes as f64 / 1_048_576.0)
    } else if bytes >= 1_024 {
        format!("{:.1}KB", bytes as f64 / 1_024.0)
    } else {
        format!("{}B", bytes)
    }
}

fn display_metrics(metrics: &MetricsResponse) -> Result<()> {
    let mut stdout = stdout();

    // Clear screen and move to top (simple approach, no raw mode needed)
    execute!(
        stdout,
        terminal::Clear(ClearType::All),
        cursor::MoveTo(0, 0)
    )?;

    // Header with global stats
    println!(
        "Cycle: {}ms ({:.2}x) | Active Logs: {}",
        metrics.global.last_cycle_duration_ms,
        metrics.global.last_cycle_fraction,
        metrics.global.last_active_log_count
    );
    println!();

    // Table header
    println!(
        "{:<20} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>12} {:>10} {:>12}",
        "LOG",
        "ROWS",
        "LEAVES",
        "TOTAL",
        "COPY",
        "QUERY",
        "INSERT",
        "FETCH",
        "TREE",
        "SIZE",
        "MEMORY",
        "UPDATE"
    );
    println!("{}", "-".repeat(144));

    // Sort logs by name for consistent display
    let mut log_names: Vec<_> = metrics.logs.keys().collect();
    log_names.sort();

    // Display each log's metrics
    for log_name in log_names {
        if let Some(log_metrics) = metrics.logs.get(log_name) {
            // Extract time from ISO8601 timestamp (HH:MM:SS)
            let time_str = log_metrics
                .last_update
                .split('T')
                .nth(1)
                .and_then(|t| t.split('.').next())
                .unwrap_or("--:--:--");

            println!(
                "{:<20} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>8} {:>12} {:>10} {:>12}",
                if log_name.len() > 20 {
                    &log_name[..20]
                } else {
                    log_name
                },
                log_metrics.last_batch_rows,
                log_metrics.last_batch_leaves,
                log_metrics.last_total_ms,
                log_metrics.last_copy_ms,
                log_metrics.last_query_sources_ms,
                log_metrics.last_insert_merkle_log_ms,
                log_metrics.last_fetch_merkle_log_ms,
                log_metrics.last_tree_update_ms,
                format_number(log_metrics.tree_size),
                format_memory(log_metrics.tree_memory_bytes),
                time_str
            );
        }
    }

    println!();
    println!(
        "Press Ctrl+C to exit | Refreshing every {}s",
        std::env::var("REFRESH_INTERVAL").unwrap_or_else(|_| "1".to_string())
    );

    stdout.flush()?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    // Get server URL from environment or use default
    let server_url =
        std::env::var("MRKL_SERVER_URL").unwrap_or_else(|_| "http://localhost:3000".to_string());
    let metrics_url = format!("{}/metrics", server_url);

    // Refresh interval in seconds
    let refresh_interval = std::env::var("REFRESH_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1);

    println!("Connecting to {}...", metrics_url);
    println!("Refresh interval: {}s", refresh_interval);
    println!();

    // No raw mode - allows normal terminal interactions (text selection, etc.)

    loop {
        match fetch_metrics(&metrics_url).await {
            Ok(metrics) => {
                if let Err(e) = display_metrics(&metrics) {
                    eprintln!("Display error: {}", e);
                }
            }
            Err(e) => {
                execute!(
                    stdout(),
                    terminal::Clear(ClearType::All),
                    cursor::MoveTo(0, 0)
                )?;
                println!("⚠️  Failed to fetch metrics: {}", e);
                println!("Retrying in {}s...", refresh_interval);
                println!("\nPress Ctrl+C to exit");
            }
        }

        tokio::time::sleep(Duration::from_secs(refresh_interval)).await;
    }
}
