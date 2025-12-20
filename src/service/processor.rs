use anyhow::Result;
use deadpool_postgres::Object as PooledConnection;

use super::{AppState, SourceConfig};
use crate::LeafHash;

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
        // Time the copy from source tables to merkle_log
        let batch_start = std::time::Instant::now();

        // Get the current merkle state and last processed ID
        let current_last_id = app_state.merkle_state.read().last_processed_id;

        // Copy pending entries into merkle_log
        let batch_result = copy_source_rows(&app_state, batch_size).await;

        let batch_duration = batch_start.elapsed();

        let Ok(rows_copied) = batch_result else {
            let err = batch_result.err().unwrap();
            if !err.to_string().contains("Another processing batch is running") {
                println!("⚠️ Error processing batch: {}", err);
            }
            continue;
        };

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

            let Ok(rows) = fetch_result else {
                println!("⚠️ Error fetching processed rows: {}", fetch_result.err().unwrap());
                continue;
            };

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

        // Wait for next interval
        tokio::time::sleep(interval).await;
    }
}

/// Load enabled source configurations from verification_sources table
async fn load_source_configs(conn: &PooledConnection) -> Result<Vec<SourceConfig>> {
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
        configs.push(SourceConfig::new(&table_name, &hash_column, &order_column));
    }

    if !configs.is_empty() {
        println!("📋 Loaded {} source config(s) from verification_sources", configs.len());
    }

    Ok(configs)
}

/// Validate that all configured source tables exist
/// Returns only the configs for tables that actually exist
async fn validate_source_tables(conn: &PooledConnection, configs: &[SourceConfig]) -> Result<Vec<SourceConfig>> {
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
async fn copy_source_rows(app_state: &AppState, batch_size: i64) -> Result<i64> {
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
    
    // Start transaction
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
