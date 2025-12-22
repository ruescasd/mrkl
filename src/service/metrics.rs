use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

/// Per-log batch processing metrics
#[derive(Debug, Clone)]
pub struct LogMetrics {
    pub log_name: String,
    /// Number of rows processed in the last batch
    pub last_batch_rows: u64,
    /// Number of leaves added to tree in the last batch
    pub last_batch_leaves: u64,
    /// Time to copy rows from source tables to merkle_log (ms)
    pub last_copy_source_rows_ms: u64,
    /// Time to fetch new entries from merkle_log (ms)
    pub last_fetch_merkle_log_ms: u64,
    /// Time to update in-memory tree (includes write lock hold time) (ms)
    pub last_tree_update_ms: u64,
    /// Total number of batches processed since startup
    pub batches_processed: u64,
    /// Current tree size (number of leaves)
    pub tree_size: u64,
    /// Estimated tree memory usage in bytes (placeholder)
    pub tree_memory_bytes: u64,
    /// Timestamp of last update
    pub last_updated: DateTime<Utc>,
}

impl LogMetrics {
    pub fn new(log_name: String) -> Self {
        Self {
            log_name,
            last_batch_rows: 0,
            last_batch_leaves: 0,
            last_copy_source_rows_ms: 0,
            last_fetch_merkle_log_ms: 0,
            last_tree_update_ms: 0,
            batches_processed: 0,
            tree_size: 0,
            tree_memory_bytes: 0,
            last_updated: Utc::now(),
        }
    }

    /// Update metrics after processing a batch
    pub fn record_batch(
        &mut self,
        rows_copied: u64,
        leaves_added: u64,
        copy_source_rows_ms: u64,
        fetch_merkle_log_ms: u64,
        tree_update_ms: u64,
        tree_size: u64,
    ) {
        self.last_batch_rows = rows_copied;
        self.last_batch_leaves = leaves_added;
        self.last_copy_source_rows_ms = copy_source_rows_ms;
        self.last_fetch_merkle_log_ms = fetch_merkle_log_ms;
        self.last_tree_update_ms = tree_update_ms;
        self.batches_processed += 1;
        self.tree_size = tree_size;
        // TODO: Calculate actual memory usage
        // Rough estimate: tree_size * (32 bytes hash + internal nodes overhead)
        self.tree_memory_bytes = tree_size * 100; // Placeholder approximation
        self.last_updated = Utc::now();
    }
}

/// Global processing metrics
#[derive(Debug, Clone)]
pub struct GlobalMetrics {
    /// Duration of last complete processing cycle (ms)
    pub last_cycle_duration_ms: u64,
    /// Number of active logs being processed
    pub last_active_log_count: usize,
    /// Ratio of cycle duration to processing interval
    pub last_cycle_fraction: f64,
    /// Timestamp when last cycle completed
    pub last_cycle_timestamp: DateTime<Utc>,
}

impl GlobalMetrics {
    pub fn new() -> Self {
        Self {
            last_cycle_duration_ms: 0,
            last_active_log_count: 0,
            last_cycle_fraction: 0.0,
            last_cycle_timestamp: Utc::now(),
        }
    }

    /// Update metrics after a processing cycle
    pub fn record_cycle(&mut self, cycle_duration_ms: u64, active_logs: usize, interval: &core::time::Duration) {
        self.last_cycle_duration_ms = cycle_duration_ms;
        self.last_active_log_count = active_logs;
        // Calculate fraction assuming a 1-second interval (configurable in processor)
        self.last_cycle_fraction = cycle_duration_ms as f64 / interval.as_millis() as f64;
        self.last_cycle_timestamp = Utc::now();
    }
}

/// Container for all metrics
pub struct Metrics {
    /// Per-log metrics, keyed by log_name
    pub log_metrics: Arc<RwLock<HashMap<String, LogMetrics>>>,
    /// Global processing metrics
    pub global_metrics: Arc<RwLock<GlobalMetrics>>,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            log_metrics: Arc::new(RwLock::new(HashMap::new())),
            global_metrics: Arc::new(RwLock::new(GlobalMetrics::new())),
        }
    }

    /// Get or create metrics for a log
    pub fn get_or_create_log_metrics(&self, log_name: &str) -> LogMetrics {
        let mut log_metrics = self.log_metrics.write();
        log_metrics
            .entry(log_name.to_string())
            .or_insert_with(|| LogMetrics::new(log_name.to_string()))
            .clone()
    }

    /// Update metrics for a specific log
    pub fn update_log_metrics<F>(&self, log_name: &str, update_fn: F)
    where
        F: FnOnce(&mut LogMetrics),
    {
        let mut log_metrics = self.log_metrics.write();
        let metrics = log_metrics
            .entry(log_name.to_string())
            .or_insert_with(|| LogMetrics::new(log_name.to_string()));
        update_fn(metrics);
    }

    /// Update global metrics
    pub fn update_global_metrics<F>(&self, update_fn: F)
    where
        F: FnOnce(&mut GlobalMetrics),
    {
        let mut global_metrics = self.global_metrics.write();
        update_fn(&mut global_metrics);
    }

    /// Get all log metrics
    pub fn get_all_log_metrics(&self) -> Vec<LogMetrics> {
        self.log_metrics.read().values().cloned().collect()
    }

    /// Get global metrics
    pub fn get_global_metrics(&self) -> GlobalMetrics {
        self.global_metrics.read().clone()
    }

    /// Get a snapshot of all metrics (logs and global)
    /// Returns (HashMap<log_name, LogMetrics>, GlobalMetrics)
    pub fn get_snapshot(&self) -> (HashMap<String, LogMetrics>, GlobalMetrics) {
        let log_metrics = self.log_metrics.read().clone();
        let global_metrics = self.global_metrics.read().clone();
        (log_metrics, global_metrics)
    }
}
