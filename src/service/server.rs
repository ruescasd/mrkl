use anyhow::Result;
use axum::{Json, Router, http::StatusCode, routing::get};
use std::net::SocketAddr;
use std::sync::Arc;
use deadpool_postgres::{Config, ManagerConfig, RecyclingMethod, Runtime};
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

/// Represents a row from a source table ready to be inserted into merkle_log
#[derive(Debug)]
struct SourceRow {
    source_table: String,
    source_id: i64,
    leaf_hash: Vec<u8>,
    order_value: i64,
}

/// Runs the batch processing loop that moves entries from source tables to merkle_log
/// and updates the merkle tree state
pub async fn run_batch_processor(app_state: AppState) {
    let batch_size: i64 = 10000;
    let interval = std::time::Duration::from_secs(1);

    loop {
        // Time the batch processing (movement from source tables to merkle_log)
        let batch_start = std::time::Instant::now();

        // Get the current merkle state and last processed ID
        let current_last_id = app_state.merkle_state.read().last_processed_id;

        // Process batch using Rust implementation
        let batch_result = process_batch_rust(&app_state, batch_size).await;

        let batch_duration = batch_start.elapsed();

        match batch_result {
            Ok(rows_copied) => {
                if rows_copied > 0 {
                    // Time the retrieval from merkle_log
                    let fetch_start = std::time::Instant::now();

                    // Query everything after our last known processed ID
                    let conn = app_state.db_pool.get().await.expect("Failed to get connection");
                    let fetch_result = conn
                        .query(
                            "SELECT id, leaf_hash
                             FROM merkle_log
                             WHERE id > $1
                             ORDER BY id",
                            &[&current_last_id],
                        )
                        .await;

                    let fetch_duration = fetch_start.elapsed();

                    match fetch_result {
                        Ok(rows) => {
                            let leaves_added = rows.len();
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
                            println!("  Rows copied: {}", rows_copied);
                            println!("  Leaves added: {}, last processed ID: {}", leaves_added, merkle_state.last_processed_id);
                            println!("  Batch processing time: {:?}", batch_duration);
                            println!("  Fetch from merkle_log time: {:?}", fetch_duration);
                            println!("  Tree construction time: {:?}", tree_duration);
                        }
                        Err(e) => println!("⚠️ Error fetching processed rows: {}", e),
                    }
                }
            }
            Err(e) => {
                if !e.to_string().contains("Another processing batch is running") {
                    println!("⚠️ Error processing batch: {}", e);
                }
            }
        }

        // Wait for next interval
        tokio::time::sleep(interval).await;
    }
}

/// Load enabled source configurations from verification_sources table
async fn load_source_configs(conn: &deadpool_postgres::Object) -> Result<Vec<super::SourceConfig>> {
    let rows = conn
        .query(
            "SELECT source_table, hash_column, order_column FROM verification_sources WHERE enabled = true ORDER BY source_table",
            &[],
        )
        .await?;

    let mut configs = Vec::new();
    for row in rows {
        let table_name: String = row.get(0);
        let hash_column: String = row.get(1);
        let order_column: String = row.get(2);
        configs.push(super::SourceConfig::new(&table_name, &hash_column, &order_column));
    }

    if !configs.is_empty() {
        println!("📋 Loaded {} source config(s) from verification_sources", configs.len());
    }

    Ok(configs)
}

/// Validate that all configured source tables exist
/// Returns only the configs for tables that actually exist
async fn validate_source_tables(conn: &deadpool_postgres::Object, configs: &[super::SourceConfig]) -> Result<Vec<super::SourceConfig>> {
    let mut valid_configs = Vec::new();
    
    for config in configs {
        // Check if table exists in database
        let exists: bool = conn
            .query_one(
                "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'public' AND table_name = $1)",
                &[&config.table_name],
            )
            .await?
            .get(0);

        if exists {
            valid_configs.push(config.clone());
        } else {
            println!("⚠️ Skipping configured source '{}' - table does not exist", config.table_name);
        }
    }
    
    Ok(valid_configs)
}

/// Process a batch of rows from configured source tables into merkle_log
/// Returns the number of rows processed
async fn process_batch_rust(app_state: &AppState, batch_size: i64) -> Result<i64> {
    // Get connection from pool
    let mut conn = app_state.db_pool.get().await?;

    // Load source configurations from database
    let source_configs = load_source_configs(&conn).await?;

    // If no sources configured, nothing to do
    if source_configs.is_empty() {
        return Ok(0);
    }

    // Filter to only tables that actually exist (skip missing tables with warning)
    let valid_configs = validate_source_tables(&conn, &source_configs).await?;
    
    // If no valid sources after filtering, nothing to do
    if valid_configs.is_empty() {
        return Ok(0);
    }
    
    // Start transaction (now works because conn is owned, not shared via Arc)
    let txn = conn.transaction().await?;

    // Advisory lock prevents concurrent execution (defensive for multi-instance deployments)
    // Note: Currently single-process, so lock has no effect, but kept for future-proofing
    let lock_acquired: bool = txn
        .query_one(
            "SELECT pg_try_advisory_xact_lock(hashtext('process_batch'))",
            &[],
        )
        .await?
        .get(0);

    if !lock_acquired {
        return Err(anyhow::anyhow!("Another processing batch is running"));
    }

    // Get next available ID in merkle_log
    let next_id: i64 = txn
        .query_one("SELECT COALESCE(MAX(id), 0) + 1 FROM merkle_log", &[])
        .await?
        .get(0);

    // Collect rows from all configured source tables
    let mut all_source_rows = Vec::new();

    for source_config in valid_configs.iter() {
        // Get last processed source_id for this specific source table
        let last_processed: i64 = txn
            .query_one(
                "SELECT COALESCE(MAX(source_id), 0) FROM merkle_log WHERE source_table = $1",
                &[&source_config.table_name],
            )
            .await?
            .get(0);

        /*println!(
            "Processing source '{}' from last_processed_id {}",
            source_config.table_name, last_processed
        );*/

        // Query unprocessed rows from this source table
        let query = format!(
            "SELECT {}, {} FROM {} WHERE {} > $1 ORDER BY {} LIMIT $2",
            source_config.order_column,
            source_config.hash_column,
            source_config.table_name,
            source_config.order_column,
            source_config.order_column
        );

        let rows = txn.query(&query, &[&last_processed, &batch_size]).await?;

        for row in rows {
            let order_value: i64 = row.get(0);
            let leaf_hash: Vec<u8> = row.get(1);

            all_source_rows.push(SourceRow {
                source_table: source_config.table_name.clone(),
                source_id: order_value, // Using order_value as source_id (assumes id column)
                leaf_hash,
                order_value,
            });
        }
    }

    // Sort all rows by order_value to maintain global ordering
    all_source_rows.sort_by_key(|r| r.order_value);

    // Limit to batch_size after merging
    all_source_rows.truncate(batch_size as usize);

    // Insert into merkle_log with sequential IDs
    let rows_inserted = all_source_rows.len() as i64;
    
    for (offset, source_row) in all_source_rows.iter().enumerate() {
        let merkle_id = next_id + offset as i64;
        
        txn.execute(
            "INSERT INTO merkle_log (id, source_table, source_id, leaf_hash) VALUES ($1, $2, $3, $4)",
            &[&merkle_id, &source_row.source_table, &source_row.source_id, &source_row.leaf_hash],
        )
        .await?;
    }

    // Commit transaction - data is now persisted
    txn.commit().await?;

    Ok(rows_inserted)
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
