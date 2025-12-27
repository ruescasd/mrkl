//! Main server binary for mrkl
//!
//! Runs the HTTP API server and batch processor that monitors `PostgreSQL` tables
//! and builds merkle trees for Certificate Transparency-style verification.
//!

#![allow(clippy::print_stdout)]
//! # Usage
//!
//! ```bash
//! cargo run --bin main
//! cargo run --bin main -- --verify-db
//! ```
//!
//! The `--verify-db` flag validates the database schema without starting the server.

use anyhow::Result;
use mrkl::service;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    // Initialize tracing subscriber
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Parse command line arguments
    let args: Vec<String> = std::env::args().collect();
    let verify_db = args.contains(&"--verify-db".to_string());

    // Initialize application state (database connection + merkle state)
    let app_state = service::initialize_app_state().await?;

    // If --verify-db flag is present, run validation and exit
    if verify_db {
        let conn = app_state.db_pool.get().await?;
        let validations = service::validate_all_logs(&conn).await?;
        service::print_validation_report(&validations);

        // Exit with status code based on validation result
        let all_valid = validations.iter().all(mrkl::service::LogValidation::is_valid);
        std::process::exit(if all_valid { 0 } else { 1 });
    }

    // Run the complete server with batch processing
    service::run_server(app_state).await?;

    Ok(())
}
