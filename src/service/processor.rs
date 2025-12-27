use anyhow::Result;
use chrono::{DateTime, Utc};
use deadpool_postgres::{Object as PooledConnection, Transaction};
use std::collections::{BTreeSet, hash_map::DefaultHasher};
use std::hash::{Hash, Hasher};

use crate::LeafHash;
use crate::service::state::AppState;

/// Statistics from a batch processing operation
#[derive(Debug, Clone)]
pub struct BatchStats {
    /// Number of rows copied from source tables to `merkle_log`
    pub rows_copied: u64,
    /// Time spent querying source tables (ms)
    pub query_sources_ms: u64,
    /// Time spent inserting into `merkle_log` (ms)
    pub insert_merkle_log_ms: u64,
}

/// CRITICAL ARCHITECTURAL NOTE: `merkle_log` is GROUND TRUTH
///
/// The ordering committed to `merkle_log` cannot be reconstructed deterministically from source tables alone.
/// This is because:
///
/// 1. **Batch boundaries matter**: Entries are sorted within each batch, not globally. The same entry might
///    end up at different positions depending on when batches run.
///
///    Example:
///
///    Entry X (no timestamp) with 9 timestamped entries in batch 1 → X at position 10
///    Same 20 entries in one batch → X at position 20 (after all 19 timestamped entries)
///
/// 2. **Late arrivals**: An entry with timestamp T1 might arrive AFTER entries with T2, T3 (T2 > T1).
///    Once T2, T3 are committed to the merkle tree, we cannot retroactively insert T1 before them.
///
/// 3. **Ordering is a point-in-time commitment**: When processing a batch, we commit to "these are all
///    the entries we know about right now, in this order." Future entries might logically belong earlier,
///    but they weren't available yet.
///
/// Therefore:
/// - Startup rebuild from `merkle_log` IS deterministic (correct behavior)
/// - Rebuild from source tables is NOT deterministic (would produce different merkle roots)
/// - `merkle_log` must be backed up and preserved for disaster recovery
/// - This is correct behavior for append-only transparency logs
///
/// UPDATE: it would be possible to make rebuilds deterministic by storing batch boundaries,
/// this would also allow the storing of only published roots instead of checkpointing after each entry.
///
/// Represents a row from a source table ready to be inserted into `merkle_log`
/// Implements Ord for universal ordering: (timestamp, id, `table_name`)
#[derive(Debug, Clone, Eq)]
struct SourceRow {
    /// Name of the source table this row came from
    source_table: String,
    /// ID of the row in the source table
    source_id: i64,
    /// SHA256 hash of the leaf data
    leaf_hash: Vec<u8>,
    /// Optional timestamp for ordering (timestamped entries sort before non-timestamped)
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
            (Some(a), Some(b)) => a
                .cmp(b)
                .then_with(|| self.source_id.cmp(&other.source_id))
                .then_with(|| self.source_table.cmp(&other.source_table)),
            (Some(_), None) => std::cmp::Ordering::Less, // Timestamped entries come first
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

/// Configuration for a source table that feeds into the merkle log
#[derive(Debug, Clone)]
pub struct SourceConfig {
    /// The name of the source table in the database
    pub table_name: String,
    /// The column containing the pre-computed hash value
    pub hash_column: String,
    /// The unique ID column (must be unique per table, e.g., primary key)
    pub id_column: String,
    /// Optional timestamp column for chronological ordering across tables
    pub timestamp_column: Option<String>,
    /// The log this source belongs to
    pub log_name: String,
}

impl SourceConfig {
    /// Creates a new source configuration
    pub fn new(
        table_name: impl Into<String>,
        hash_column: impl Into<String>,
        id_column: impl Into<String>,
        timestamp_column: Option<String>,
        log_name: impl Into<String>,
    ) -> Self {
        Self {
            table_name: table_name.into(),
            hash_column: hash_column.into(),
            id_column: id_column.into(),
            timestamp_column,
            log_name: log_name.into(),
        }
    }
}

/// Runs the batch processing loop that moves entries from source tables to `merkle_log`
/// and updates the merkle tree state for each log
#[allow(clippy::cognitive_complexity)]
pub async fn run_batch_processor(app_state: AppState) {
    let batch_size: i64 = 10000;
    let interval = std::time::Duration::from_secs(1);

    loop {
        // Check processor state for pause/stop control
        let state_value = app_state
            .processor_state
            .load(std::sync::atomic::Ordering::Relaxed);
        match state_value {
            1 => {
                // Paused - sleep and continue
                tracing::info!("Batch processor paused");
                tokio::time::sleep(interval).await;
                continue;
            }
            2 => {
                // Stopping - break out of loop
                tracing::info!("Batch processor stopping");
                break;
            }
            _ => {
                // Running - continue normally
            }
        }

        // Load all enabled logs from the database
        let conn = match app_state.db_pool.get().await {
            Ok(c) => c,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to get connection for loading logs");
                tokio::time::sleep(interval).await;
                continue;
            }
        };

        let logs_result = load_enabled_logs(&conn).await;
        // Release connection before processing
        drop(conn);

        let log_names = match logs_result {
            Ok(names) => names,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to load enabled logs");
                tokio::time::sleep(interval).await;
                continue;
            }
        };

        let now = std::time::Instant::now();
        // Process each log independently
        for log_name in log_names.clone() {
            if let Err(e) = process_log(&app_state, &log_name, batch_size).await
                && !e
                    .to_string()
                    .contains("Another processing batch is running")
            {
                tracing::warn!(log_name, error = ?e, "Error processing log");
            }
        }
        let elapsed = now.elapsed();

        // Update global metrics
        app_state.metrics.update_global_metrics(|global| {
            global.record_cycle(elapsed.as_millis() as u64, log_names.len(), &interval);
        });

        // Wait for next interval
        tokio::time::sleep(interval).await;
    }

    tracing::info!("Batch processor stopped");
}

/// Process a single log: copy rows from source tables to `merkle_log` and update tree
async fn process_log(app_state: &AppState, log_name: &str, batch_size: i64) -> Result<()> {
    // Time the entire processing cycle
    let total_start = std::time::Instant::now();

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

    // Get the current merkle state and last processed ID
    let current_last_id = merkle_state_arc.read().last_processed_id;

    // Time the copy from source tables to merkle_log
    let copy_start = std::time::Instant::now();

    // Copy pending entries into merkle_log
    let batch_result = copy_source_rows(app_state, log_name, batch_size).await;

    let copy_duration = copy_start.elapsed();

    let batch_stats = batch_result?;

    if batch_stats.rows_copied > 0 {
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

        let rows = fetch_result?;

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
        // for the moment we must checkpoint after each addition,
        // because in a rebuild scenario we do not know which roots have been published
        // these checkpoints are currently part of update_with_entry
        // merkle_state.root_checkpoint();

        let tree_duration = tree_start.elapsed();

        // Update metrics for this log
        let final_tree_size = merkle_state.tree.len();
        // Release lock asap
        drop(merkle_state);

        let total_duration = total_start.elapsed();

        app_state
            .metrics
            .update_log_metrics(log_name, |log_metrics| {
                log_metrics.record_batch(
                    batch_stats.rows_copied as u64,
                    leaves_added as u64,
                    total_duration.as_millis() as u64,
                    copy_duration.as_millis() as u64,
                    batch_stats.query_sources_ms,
                    batch_stats.insert_merkle_log_ms,
                    fetch_duration.as_millis() as u64,
                    tree_duration.as_millis() as u64,
                    final_tree_size,
                );
            });
    } else {
        // Update metrics even when no rows copied (caught up)
        let tree_size = merkle_state_arc.read().tree.len();
        let total_duration = total_start.elapsed();

        app_state
            .metrics
            .update_log_metrics(log_name, |log_metrics| {
                log_metrics.record_batch(
                    0,
                    0,
                    total_duration.as_millis() as u64,
                    copy_duration.as_millis() as u64,
                    0,
                    0,
                    0,
                    0,
                    tree_size,
                );
            });
    }

    Ok(())
}

/// When using bulk inserts, we process in chunks to avoid postgresql parameter limit 
/// (~32K (probably 32,767) parameters = ~6.5K rows × 5 params)
/// 1000 rows × 5 params = 5000 parameters (well under limit)
const CHUNK_SIZE: usize = 1000;

/// Process a batch of rows from configured source tables into `merkle_log` for a specific log
/// Returns statistics about the batch processing operation
#[allow(clippy::cognitive_complexity)]
async fn copy_source_rows(
    app_state: &AppState,
    log_name: &str,
    batch_size: i64,
) -> Result<BatchStats> {
    // Get connection from pool
    let mut conn = app_state.db_pool.get().await?;

    // Load source configurations from database for this log
    let source_configs = load_source_configs(&conn, log_name).await?;

    // If no sources configured, nothing to do
    if source_configs.is_empty() {
        return Ok(BatchStats {
            rows_copied: 0,
            query_sources_ms: 0,
            insert_merkle_log_ms: 0,
        });
    }

    // Filter to only tables that actually exist (skip missing tables with warning)
    let valid_configs = validate_source_tables(&conn, &source_configs).await?;

    // If no valid sources after filtering, nothing to do
    if valid_configs.is_empty() {
        return Ok(BatchStats {
            rows_copied: 0,
            query_sources_ms: 0,
            insert_merkle_log_ms: 0,
        });
    }

    // Start transaction
    let txn = conn.transaction().await?;

    // Advisory lock prevents concurrent execution for this specific log
    // Use log name to generate unique lock ID
    let lock_id = hash_string_to_i64(log_name);
    let lock_acquired: bool = txn
        .query_one("SELECT pg_try_advisory_xact_lock($1)", &[&lock_id])
        .await?
        .get(0);

    if !lock_acquired {
        return Err(anyhow::anyhow!(
            "Another processing batch is running for log '{log_name}'"
        ));
    }

    let query_start = std::time::Instant::now();
    let rows_to_insert = get_source_rows(&txn, valid_configs, log_name, batch_size).await?;
    let query_duration = query_start.elapsed();

    // Get next available ID in merkle_log (globally unique across all logs)
    let next_id: i64 = txn
        .query_one("SELECT COALESCE(MAX(id), 0) + 1 FROM merkle_log", &[])
        .await?
        .get(0);
    
    let insert_start = std::time::Instant::now();

    if !rows_to_insert.is_empty() {
        // Insert into merkle_log with sequential IDs using multi-row INSERT
        // Process in chunks to avoid PostgreSQL parameter limit (~32K (probably 32,767) parameters = ~6.5K rows × 5 params)
        for (chunk_idx, chunk) in rows_to_insert.chunks(CHUNK_SIZE).enumerate() {
            // usize::MAX * usize::MAX << u64::MAX
            #[allow(clippy::arithmetic_side_effects)]
            let chunk_offset: u64 = chunk_idx as u64 * CHUNK_SIZE as u64;

            // Build multi-row INSERT statement for this chunk
            // INSERT INTO merkle_log (...) VALUES ($1,$2,$3,$4,$5), ($6,$7,$8,$9,$10), ...
            let mut query = String::from(
                "INSERT INTO merkle_log (id, log_name, source_table, source_id, leaf_hash) VALUES ",
            );
            let mut params: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = Vec::new();

            for (offset, _) in chunk.iter().enumerate() {
                if offset > 0 {
                    query.push_str(", ");
                }
                // CHUNK_SIZE * 5 << usize::MAX
                #[allow(clippy::arithmetic_side_effects)]
                let base = offset * 5;
                // CHUNK_SIZE * 5 << usize::MAX
                #[allow(clippy::arithmetic_side_effects)]
                query.push_str(&format!(
                    "(${}, ${}, ${}, ${}, ${})",
                    base + 1,
                    base + 2,
                    base + 3,
                    base + 4,
                    base + 5
                ));
            }

            // Flatten parameters for this chunk: for each row, add (id, log_name, source_table, source_id, leaf_hash)
            let mut param_values: Vec<(i64, &str, &str, i64, &[u8])> = Vec::new();
            for (offset, source_row) in chunk.iter().enumerate() {
                // the total number of log entries will never approach i64::MAX in practice
                #[allow(clippy::arithmetic_side_effects)]
                let merkle_id = next_id + (chunk_offset + offset as u64) as i64;
                param_values.push((
                    merkle_id,
                    log_name,
                    &source_row.source_table,
                    source_row.source_id,
                    &source_row.leaf_hash,
                ));
            }

            // Build params vector with correct types
            for pv in &param_values {
                params.push(&pv.0); // id
                params.push(&pv.1); // log_name
                params.push(&pv.2); // source_table
                params.push(&pv.3); // source_id
                params.push(&pv.4); // leaf_hash
            }

            txn.execute(&query, &params).await?;
        }
    }

    let insert_duration = insert_start.elapsed();
    txn.commit().await?;

    Ok(BatchStats {
        rows_copied: rows_to_insert.len() as u64,
        query_sources_ms: query_duration.as_millis() as u64,
        insert_merkle_log_ms: insert_duration.as_millis() as u64,
    })
}

/// Retrieves rows from all valid source tables for a specific log, up to `batch_size` total
async fn get_source_rows(txn: &Transaction<'_>, valid_configs: Vec<SourceConfig>, log_name: &str, batch_size: i64) -> Result<Vec<SourceRow>> {

    // Collect rows from all configured source tables into a BTreeSet for automatic sorting
    let mut all_source_rows = BTreeSet::new();

    for source_config in &valid_configs {
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
            let order_timestamp: Option<DateTime<Utc>> = if source_config.timestamp_column.is_some()
            {
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
    let rows_to_insert: Vec<_> = all_source_rows
        .into_iter()
        .take(batch_size as usize)
        .collect();

    Ok(rows_to_insert)
    
}

/// Rebuilds all enabled logs from the database on startup
///
/// # Errors
///
/// Returns an error if any database operations fail, including getting a connection,
/// loading log configurations, validating sources, or rebuilding individual logs.
#[allow(clippy::cognitive_complexity)]
pub async fn rebuild_all_logs(app_state: &AppState) -> Result<()> {
    tracing::info!("Rebuilding all logs from database");

    let conn = app_state.db_pool.get().await?;
    let log_names = load_enabled_logs(&conn).await?;

    // Validate all source configurations and report issues
    tracing::info!("Validating source configurations");
    let validations = crate::service::validate_all_logs(&conn).await?;
    drop(conn);

    for validation in &validations {
        if !validation.enabled {
            // Skip disabled logs
            continue;
        }

        let invalid_sources: Vec<_> = validation
            .sources
            .iter()
            .filter(|s| !s.is_valid())
            .collect();

        if !invalid_sources.is_empty() {
            tracing::warn!(
                log_name = validation.log_name,
                invalid_count = invalid_sources.len(),
                "Log has invalid sources"
            );
            for source in invalid_sources {
                for error in source.errors() {
                    tracing::warn!(
                        source_table = source.source_table,
                        error,
                        "Source validation error"
                    );
                }
            }
        }
    }
    tracing::info!("Validation complete");

    if log_names.is_empty() {
        tracing::info!("No logs to rebuild");
        return Ok(());
    }

    for log_name in &log_names {
        rebuild_log(app_state, log_name).await?;
    }

    tracing::info!(log_count = log_names.len(), "Rebuilt logs");
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
        tracing::debug!(log_name, "Log has no entries");
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
    // for the moment we must checkpoint after each addition,
    // because in a rebuild scenario we do not know which roots have been published
    // these checkpoints are currently part of update_with_entry
    // merkle_state.root_checkpoint();

    // Store in DashMap
    let merkle_state_arc = std::sync::Arc::new(parking_lot::RwLock::new(merkle_state));
    let tree_size = merkle_state_arc.read().tree.len();
    let last_id = merkle_state_arc.read().last_processed_id;

    app_state
        .merkle_states
        .insert(log_name.to_string(), merkle_state_arc);

    tracing::info!(log_name, tree_size, last_id, "Log rebuilt");

    Ok(())
}

/// Load enabled log names from `verification_logs` table
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

/// Load enabled source configurations from `verification_sources` table for a specific log
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
        let log_name_: String = row.get(4);
        configs.push(SourceConfig::new(
            &table_name,
            &hash_column,
            &id_column,
            timestamp_column,
            &log_name_,
        ));
    }

    if !configs.is_empty() {
        // println!("📋 Loaded {} source config(s) for log '{}'", configs.len(), log_name);
    }

    Ok(configs)
}

/// Validate that all configured source tables exist
/// Returns only the configs for tables that actually exist
async fn validate_source_tables(
    conn: &PooledConnection,
    configs: &[SourceConfig],
) -> Result<Vec<SourceConfig>> {
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
            tracing::warn!(
                source_table = config.table_name,
                "Skipping configured source - table does not exist"
            );
        }
    }

    Ok(valid_configs)
}

/// Hash a string to i64 for use as advisory lock ID
fn hash_string_to_i64(s: &str) -> i64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish() as i64
}
