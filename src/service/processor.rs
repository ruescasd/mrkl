use anyhow::Result;
use chrono::{DateTime, Utc};
use deadpool_postgres::Object as PooledConnection;
use std::collections::{hash_map::DefaultHasher, BTreeSet};
use std::hash::{Hash, Hasher};

use super::{AppState, SourceConfig};
use crate::LeafHash;

/// Represents a row from a source table ready to be inserted into merkle_log
/// Implements Ord for universal ordering: (timestamp, id, table_name)
#[derive(Debug, Clone, Eq)]
struct SourceRow {
    source_table: String,
    source_id: i64,
    leaf_hash: Vec<u8>,
    order_timestamp: Option<DateTime<Utc>>,
}

impl PartialEq for SourceRow {
    fn eq(&self, other: &Self) -> bool {
        self.order_timestamp == other.order_timestamp
            && self.source_id == other.source_id
            && self.source_table == other.source_table
    }
}

impl Ord for SourceRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Primary: timestamp (None sorts after Some for our use case)
        // Secondary: source_id (guaranteed unique per table)
        // Tertiary: source_table (for complete determinism)
        match (&self.order_timestamp, &other.order_timestamp) {
            (Some(a), Some(b)) => {
                a.cmp(b)
                    .then_with(|| self.source_id.cmp(&other.source_id))
                    .then_with(|| self.source_table.cmp(&other.source_table))
            }
            (Some(_), None) => std::cmp::Ordering::Less,  // Timestamped entries come first
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => {
                // Both have no timestamp, sort by id then table
                self.source_id
                    .cmp(&other.source_id)
                    .then_with(|| self.source_table.cmp(&other.source_table))
            }
        }
    }
}

impl PartialOrd for SourceRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

/// Runs the batch processing loop that moves entries from source tables to merkle_log
/// and updates the merkle tree state for each log
pub async fn run_batch_processor(app_state: AppState) {
    let batch_size: i64 = 10000;
    let interval = std::time::Duration::from_secs(1);

    loop {
        // Load all enabled logs from the database
        let conn = match app_state.db_pool.get().await {
            Ok(c) => c,
            Err(e) => {
                println!("⚠️ Failed to get connection for loading logs: {}", e);
                tokio::time::sleep(interval).await;
                continue;
            }
        };

        let logs_result = load_enabled_logs(&conn).await;
        drop(conn); // Release connection before processing

        let Ok(log_names) = logs_result else {
            println!("⚠️ Failed to load enabled logs: {}", logs_result.err().unwrap());
            tokio::time::sleep(interval).await;
            continue;
        };

        // Process each log independently
        for log_name in log_names {
            if let Err(e) = process_log(&app_state, &log_name, batch_size).await {
                if !e.to_string().contains("Another processing batch is running") {
                    println!("⚠️ Error processing log '{}': {}", log_name, e);
                }
            }
        }

        // Wait for next interval
        tokio::time::sleep(interval).await;
    }
}

/// Process a single log: copy rows from source tables to merkle_log and update tree
async fn process_log(app_state: &AppState, log_name: &str, batch_size: i64) -> Result<()> {
    // Ensure merkle state exists for this log (create if first time)
    if !app_state.merkle_states.contains_key(log_name) {
        app_state.merkle_states.insert(
            log_name.to_string(),
            std::sync::Arc::new(parking_lot::RwLock::new(crate::service::MerkleState::new())),
        );
    }

    let merkle_state_arc = app_state
        .merkle_states
        .get(log_name)
        .expect("merkle state should exist")
        .clone();

    // Time the copy from source tables to merkle_log
    let batch_start = std::time::Instant::now();

    // Get the current merkle state and last processed ID
    let current_last_id = merkle_state_arc.read().last_processed_id;

    // Copy pending entries into merkle_log
    let batch_result = copy_source_rows(app_state, log_name, batch_size).await;

    let batch_duration = batch_start.elapsed();

    let Ok(rows_copied) = batch_result else {
        return Err(batch_result.err().unwrap());
    };

    if rows_copied > 0 {
        // Time the retrieval from merkle_log
        let fetch_start = std::time::Instant::now();

        // Query everything after our last known processed ID for this log
        let conn = app_state.db_pool.get().await?;
        let fetch_result = conn
            .query(
                "SELECT id, leaf_hash
                    FROM merkle_log
                    WHERE log_name = $1 AND id > $2
                    ORDER BY id",
                &[&log_name, &current_last_id],
            )
            .await;

        let fetch_duration = fetch_start.elapsed();

        let Ok(rows) = fetch_result else {
            return Err(anyhow::anyhow!("Failed to fetch processed rows: {}", fetch_result.err().unwrap()));
        };

        let leaves_added = rows.len();
        // Start timing the tree construction
        let tree_start = std::time::Instant::now();

        // Update the merkle state and maps
        let mut merkle_state = merkle_state_arc.write();

        for row in rows {
            let id: i64 = row.get("id");
            let hash: Vec<u8> = row.get("leaf_hash");
            let leaf_hash = LeafHash::new(hash);

            // Update tree and ID atomically
            merkle_state.update_with_entry(leaf_hash, id);
        }

        let tree_duration = tree_start.elapsed();

        // Log timing information
        println!("Batch stats for log '{}':", log_name);
        println!("  Rows copied: {}", rows_copied);
        println!("  Leaves added: {}, last processed ID: {}", leaves_added, merkle_state.last_processed_id);
        println!("  Batch processing time: {:?}", batch_duration);
        println!("  Fetch from merkle_log time: {:?}", fetch_duration);
        println!("  Tree construction time: {:?}", tree_duration);
    }

    Ok(())
}

/// Load enabled log names from verification_logs table
async fn load_enabled_logs(conn: &PooledConnection) -> Result<Vec<String>> {
    let rows = conn
        .query(
            "SELECT log_name FROM verification_logs WHERE enabled = true ORDER BY log_name",
            &[],
        )
        .await?;

    let log_names: Vec<String> = rows.iter().map(|row| row.get(0)).collect();

    if !log_names.is_empty() {
        // println!("📋 Loaded {} enabled log(s)", log_names.len());
    }

    Ok(log_names)
}

/// Rebuilds all enabled logs from the database on startup
/// This ensures in-memory trees match persistent state
pub async fn rebuild_all_logs(app_state: &AppState) -> Result<()> {
    println!("🔄 Rebuilding all logs from database...");
    
    let conn = app_state.db_pool.get().await?;
    let log_names = load_enabled_logs(&conn).await?;
    drop(conn);
    
    if log_names.is_empty() {
        println!("✅ No logs to rebuild");
        return Ok(());
    }
    
    for log_name in &log_names {
        rebuild_log(app_state, log_name).await?;
    }
    
    println!("✅ Rebuilt {} log(s)", log_names.len());
    Ok(())
}

/// Rebuilds a single log's merkle tree from the database
async fn rebuild_log(app_state: &AppState, log_name: &str) -> Result<()> {
    let conn = app_state.db_pool.get().await?;
    
    // Fetch all entries for this log
    let rows = conn
        .query(
            "SELECT id, leaf_hash FROM merkle_log WHERE log_name = $1 ORDER BY id",
            &[&log_name],
        )
        .await?;
    
    if rows.is_empty() {
        println!("  📋 Log '{}': 0 entries", log_name);
        return Ok(());
    }
    
    // Create new merkle state
    let mut merkle_state = crate::service::MerkleState::new();
    
    for row in rows {
        let id: i64 = row.get(0);
        let hash: Vec<u8> = row.get(1);
        let leaf_hash = LeafHash::new(hash);
        merkle_state.update_with_entry(leaf_hash, id);
    }
    
    // Store in DashMap
    let merkle_state_arc = std::sync::Arc::new(parking_lot::RwLock::new(merkle_state));
    let tree_size = merkle_state_arc.read().tree.len();
    let last_id = merkle_state_arc.read().last_processed_id;
    
    app_state.merkle_states.insert(log_name.to_string(), merkle_state_arc);
    
    println!("  📋 Log '{}': {} entries (last_id: {})", log_name, tree_size, last_id);
    
    Ok(())
}

/// Load enabled source configurations from verification_sources table for a specific log
async fn load_source_configs(conn: &PooledConnection, log_name: &str) -> Result<Vec<SourceConfig>> {
    let rows = conn
        .query(
            "SELECT source_table, hash_column, id_column, timestamp_column, log_name 
             FROM verification_sources 
             WHERE enabled = true AND log_name = $1 
             ORDER BY source_table",
            &[&log_name],
        )
        .await?;

    let mut configs = Vec::new();
    for row in rows {
        let table_name: String = row.get(0);
        let hash_column: String = row.get(1);
        let id_column: String = row.get(2);
        let timestamp_column: Option<String> = row.get(3);
        let log_name: String = row.get(4);
        configs.push(SourceConfig::new(&table_name, &hash_column, &id_column, timestamp_column, &log_name));
    }

    if !configs.is_empty() {
        // println!("📋 Loaded {} source config(s) for log '{}'", configs.len(), log_name);
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

/// Process a batch of rows from configured source tables into merkle_log for a specific log
/// Returns the number of rows processed
async fn copy_source_rows(app_state: &AppState, log_name: &str, batch_size: i64) -> Result<i64> {
    // Get connection from pool
    let mut conn = app_state.db_pool.get().await?;

    // Load source configurations from database for this log
    let source_configs = load_source_configs(&conn, log_name).await?;

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

    // Advisory lock prevents concurrent execution for this specific log
    // Use log name to generate unique lock ID
    let lock_id = hash_string_to_i64(log_name);
    let lock_acquired: bool = txn
        .query_one(
            "SELECT pg_try_advisory_xact_lock($1)",
            &[&lock_id],
        )
        .await?
        .get(0);

    if !lock_acquired {
        return Err(anyhow::anyhow!("Another processing batch is running for log '{}'", log_name));
    }

    // Get next available ID in merkle_log (globally unique across all logs)
    let next_id: i64 = txn
        .query_one("SELECT COALESCE(MAX(id), 0) + 1 FROM merkle_log", &[])
        .await?
        .get(0);

    // Collect rows from all configured source tables into a BTreeSet for automatic sorting
    let mut all_source_rows = BTreeSet::new();

    for source_config in valid_configs.iter() {
        // Get last processed source_id for this specific source table in this log
        let last_processed: i64 = txn
            .query_one(
                "SELECT COALESCE(MAX(source_id), 0) FROM merkle_log WHERE log_name = $1 AND source_table = $2",
                &[&log_name, &source_config.table_name],
            )
            .await?
            .get(0);

        // Build query based on whether timestamp column is configured
        let query = if let Some(ref timestamp_col) = source_config.timestamp_column {
            format!(
                "SELECT {}, {}, {} FROM {} WHERE {} > $1 ORDER BY {} LIMIT $2",
                source_config.id_column,
                source_config.hash_column,
                timestamp_col,
                source_config.table_name,
                source_config.id_column,
                source_config.id_column
            )
        } else {
            format!(
                "SELECT {}, {} FROM {} WHERE {} > $1 ORDER BY {} LIMIT $2",
                source_config.id_column,
                source_config.hash_column,
                source_config.table_name,
                source_config.id_column,
                source_config.id_column
            )
        };

        let rows = txn.query(&query, &[&last_processed, &batch_size]).await?;

        for row in rows {
            let source_id: i64 = row.get(0);
            let leaf_hash: Vec<u8> = row.get(1);
            let order_timestamp: Option<DateTime<Utc>> = if source_config.timestamp_column.is_some() {
                row.get(2)
            } else {
                None
            };

            all_source_rows.insert(SourceRow {
                source_table: source_config.table_name.clone(),
                source_id,
                leaf_hash,
                order_timestamp,
            });
        }
    }

    // BTreeSet automatically maintains sorted order, limit to batch_size after merging
    let rows_to_insert: Vec<_> = all_source_rows.into_iter().take(batch_size as usize).collect();

    // Insert into merkle_log with sequential IDs
    let rows_inserted = rows_to_insert.len() as i64;
    
    for (offset, source_row) in rows_to_insert.iter().enumerate() {
        let merkle_id = next_id + offset as i64;
        
        txn.execute(
            "INSERT INTO merkle_log (id, log_name, source_table, source_id, leaf_hash) VALUES ($1, $2, $3, $4, $5)",
            &[&merkle_id, &log_name, &source_row.source_table, &source_row.source_id, &source_row.leaf_hash],
        )
        .await?;
    }

    // Commit transaction - data is now persisted
    txn.commit().await?;

    Ok(rows_inserted)
}

/// Hash a string to i64 for use as advisory lock ID
fn hash_string_to_i64(s: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish() as i64
}
