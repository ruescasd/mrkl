use anyhow::Result;
use axum::{Json, Router, http::StatusCode, routing::get};
use std::net::SocketAddr;
use std::sync::Arc;
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;

use super::{AppState, MerkleState};

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

/// Initialize the application state with database connection pool and empty merkle state
pub async fn initialize_app_state() -> Result<AppState> {
    println!("Connecting to database...");

    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    
    // Parse connection string into deadpool config
    let mut cfg = Config::new();
    cfg.url = Some(db_url);
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });
    
    // Create connection pool
    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;

    // Source configs are now loaded dynamically from verification_sources table
    // on each batch iteration (no hardcoded sources)

    // Initialize shared state with empty merkle state
    let app_state = AppState {
        merkle_state: Arc::new(parking_lot::RwLock::new(MerkleState::new())),
        db_pool: pool,
        source_configs: vec![], // Loaded dynamically in batch processor
    };

    Ok(app_state)
}

/// Run the complete HTTP server with batch processing
pub async fn run_server(app_state: AppState) -> Result<()> {
    // Clone the state for the processing task
    let process_state = app_state.clone();

    // Spawn the batch processing task
    let processor = tokio::spawn(async move {
        crate::service::processor::run_batch_processor(process_state).await;
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
