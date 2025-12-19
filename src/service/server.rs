use anyhow::Result;
use axum::{Json, Router, http::StatusCode, routing::get};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_postgres::NoTls;

use super::{AppState, MerkleState};
use crate::LeafHash;

/// Creates and configures the HTTP server with all routes
pub fn create_server(app_state: AppState) -> Router {
    // Fallback handler for unmatched routes
    async fn handle_unmatched() -> (StatusCode, Json<serde_json::Value>) {
        println!("🚫 Unmatched route accessed");
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "status": "error",
                "error": "Route not found. Available endpoints: /root, /rebuild, /proof, /consistency"
            })),
        )
    }

    // Build our application with routes and error handling
    Router::new()
        .route("/root", get(crate::service::get_merkle_root))
        .route("/rebuild", get(crate::service::trigger_rebuild))
        .route("/proof", get(crate::service::get_inclusion_proof))
        .route("/consistency", get(crate::service::get_consistency_proof))
        .with_state(app_state)
        .fallback(handle_unmatched)
}

/// Runs the batch processing loop that moves entries from append_only_log to processed_log
/// and updates the merkle tree state
pub async fn run_batch_processor(app_state: AppState) {
    let batch_size = 10000;
    let interval = std::time::Duration::from_secs(1);

    loop {
        // Time the batch processing (movement from append_only_log to processed_log)
        let batch_start = std::time::Instant::now();

        // Get the current merkle state and last processed ID
        let current_last_id = app_state.merkle_state.read().last_processed_id;

        let batch_result = app_state
            .client
            .query_one("SELECT * FROM process_next_batch($1)", &[&batch_size])
            .await;

        let batch_duration = batch_start.elapsed();

        match batch_result {
            Ok(row) => {
                let rows_processed: i64 = row.get("rows_processed");
                if rows_processed > 0 {
                    // Time the retrieval from processed_log
                    let fetch_start = std::time::Instant::now();

                    // Query everything after our last known processed ID
                    let fetch_result = app_state
                        .client
                        .query(
                            "SELECT id, leaf_hash
                             FROM processed_log
                             WHERE id > $1
                             ORDER BY id",
                            &[&current_last_id],
                        )
                        .await;

                    let fetch_duration = fetch_start.elapsed();

                    match fetch_result {
                        Ok(rows) => {
                            // Start timing the tree construction
                            let tree_start = std::time::Instant::now();

                            // Update the merkle state and maps
                            let mut merkle_state = app_state.merkle_state.write();

                            for row in rows {
                                let id: i64 = row.get("id");
                                let hash: Vec<u8> = row.get("leaf_hash");
                                let leaf_hash = LeafHash::new(hash);

                                // Update tree and ID atomically
                                merkle_state.update_with_entry(leaf_hash, id);
                            }

                            let tree_duration = tree_start.elapsed();

                            // Log timing information
                            println!("Batch stats:");
                            println!("  Rows processed: {}", rows_processed);
                            println!("  Batch processing time: {:?}", batch_duration);
                            println!("  Fetch from processed_log time: {:?}", fetch_duration);
                            println!("  Tree construction time: {:?}", tree_duration);
                        }
                        Err(e) => println!("⚠️ Error fetching processed rows: {}", e),
                    }
                }
            }
            Err(e) => {
                if !e
                    .to_string()
                    .contains("Another processing batch is running")
                {
                    println!("⚠️ Error processing batch: {}", e);
                }
            }
        }

        // Wait for next interval
        tokio::time::sleep(interval).await;
    }
}

/// Initialize the application state with database connection and empty merkle state
pub async fn initialize_app_state() -> Result<AppState> {
    println!("Connecting to database...");

    let (client, connection) = tokio_postgres::connect(
        &std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
        NoTls,
    )
    .await?;

    // Wrap the client in an Arc so it can be shared between tasks
    let client = Arc::new(client);

    // Spawn a task to drive the connection
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Initialize shared state with empty merkle state
    let app_state = AppState {
        merkle_state: Arc::new(parking_lot::RwLock::new(MerkleState::new())),
        client: client.clone(),
    };

    Ok(app_state)
}

/// Run the complete HTTP server with batch processing
pub async fn run_server(app_state: AppState) -> Result<()> {
    // Clone the state for the processing task
    let process_state = app_state.clone();

    // Spawn the batch processing task
    let processor = tokio::spawn(async move {
        run_batch_processor(process_state).await;
    });

    // Do initial rebuild of the tree now that connection is established
    println!("Performing initial tree rebuild...");
    match crate::service::routes::rebuild_tree(&app_state).await {
        Ok((size, _, last_id)) => {
            println!(
                "✅ Initial rebuild complete with {} entries (last_id: {})",
                size, last_id
            );
        }
        Err(e) => println!("⚠️ Failed to perform initial rebuild: {}", e),
    }

    // Create the server
    let app = create_server(app_state);

    // Run our HTTP server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("🌐 HTTP server listening on http://{}", addr);
    let server = axum::serve(tokio::net::TcpListener::bind(addr).await?, app);

    // Wait for Ctrl+C, processor, or server to finish
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down...");
        }
        result = processor => {
            match result {
                Ok(_) => println!("Processor completed successfully"),
                Err(e) => println!("Processor error: {}", e),
            }
        }
        _ = server => {
            println!("HTTP server shut down");
        }
    }

    Ok(())
}
