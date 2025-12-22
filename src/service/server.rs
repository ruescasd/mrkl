use anyhow::Result;
use axum::{Json, Router, http::StatusCode, routing::get};
use dashmap::DashMap;
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio_postgres::NoTls;

use crate::service::state::AppState;

/// Creates and configures the HTTP server with all routes
pub fn create_server(app_state: AppState) -> Router {
    // Fallback handler for unmatched routes
    async fn handle_unmatched() -> (StatusCode, Json<serde_json::Value>) {
        println!("🚫 Unmatched route accessed");
        (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "status": "error",
                "error": "Route not found. Available endpoints: /logs/{log_name}/root, /logs/{log_name}/size, /logs/{log_name}/proof, /logs/{log_name}/consistency, /logs/{log_name}/has_leaf, /logs/{log_name}/has_root"
            })),
        )
    }

    // Build our application with routes and error handling
    Router::new()
        .route("/logs/:log_name/root", get(crate::service::get_merkle_root))
        .route("/logs/:log_name/size", get(crate::service::get_log_size))
        .route(
            "/logs/:log_name/proof",
            get(crate::service::get_inclusion_proof),
        )
        .route(
            "/logs/:log_name/consistency",
            get(crate::service::get_consistency_proof),
        )
        .route("/logs/:log_name/has_leaf", get(crate::service::has_leaf))
        .route("/logs/:log_name/has_root", get(crate::service::has_root))
        .with_state(app_state)
        .fallback(handle_unmatched)
}

/// Initialize the application state with database connection pool and empty merkle states map
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

    // Initialize shared state with empty DashMap for merkle states
    // Logs will be loaded dynamically from verification_logs table
    let app_state = AppState {
        merkle_states: Arc::new(DashMap::new()),
        db_pool: pool,
    };

    Ok(app_state)
}

/// Run the complete HTTP server with batch processing
pub async fn run_server(app_state: AppState) -> Result<()> {
    // Clone the state for the processing task
    let process_state = app_state.clone();

    // Rebuild all logs from database on startup to ensure in-memory trees match persistent state
    crate::service::rebuild_all_logs(&app_state).await?;

    // Spawn the batch processing task
    let processor = tokio::spawn(async move {
        crate::service::processor::run_batch_processor(process_state).await;
    });

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
