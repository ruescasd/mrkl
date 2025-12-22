# Merkle Tree PostgreSQL Integration - TODO List

## ✅ Completed

### Phase 0: Code Cleanup & Foundation
- [x] Renamed tables for clarity (processed_log → merkle_log, append_only_log → source_log)
- [x] Removed unused SQL objects (analyze_next_batch, processing_status, validate_processed_sequence)
- [x] Added source_table column to merkle_log for multi-source tracking
- [x] Changed source_log.id to BIGSERIAL for type consistency (all i64)

### Phase 1: Single Log, Multiple Sources
- [x] Implemented SourceConfig struct for configurable source tables
- [x] Replaced PL/pgSQL process_next_batch() with Rust implementation
- [x] Implemented batch processor that queries multiple source tables
- [x] Merges rows by order column and inserts with sequential merkle_log IDs
- [x] Implemented connection pooling (deadpool-postgres)
- [x] Fixed all type conversions (consistent i64 throughout)
- [x] **Created verification_sources configuration table**
- [x] **Dynamic source loading on every batch iteration**
- [x] **All-or-nothing validation of configured tables**
- [x] **Automated multi-source testing infrastructure**
- [x] All tests passing

**Phase 1 Complete!** ✨

### Phase 2: Multiple Independent Logs
**Goal**: Support multiple independent merkle trees, each tracking different sets of source tables

**Implementation Complete**:
- [x] Created `verification_logs` table (log_name PRIMARY KEY, enabled BOOLEAN)
- [x] Extended `verification_sources` with log_name (composite PK: source_table, log_name)
- [x] Added log_name FK to merkle_log with CASCADE delete
- [x] Implemented DashMap<String, Arc<RwLock<MerkleState>>> for per-log state
- [x] Path-based HTTP endpoints: /logs/{log_name}/root, /size, /proof, /consistency
- [x] Batch processor loops over all enabled logs, processes each independently
- [x] Per-log advisory locks using hash_string_to_i64(log_name)
- [x] Composite key sorting: (order_value, source_table, source_id) for deterministic ordering
- [x] Startup rebuild: all enabled logs rebuilt from database on server start
- [x] Clean architectural separation: batch processor owns all DB access, HTTP handlers read from DashMap only
- [x] Removed manual /rebuild endpoint (no longer needed with startup rebuild + continuous processing)
- [x] Idempotent test setup with two separate test logs (no destructive operations)
- [x] All tests passing, repeatable from any database state

**Phase 2 Complete!** ✨

**Architecture Notes**:
- merkle_log.id is a global SERIAL sequence shared across all logs
- IDs are monotonically increasing per log but not sequential (gaps from other logs)
- Each log maintains independent merkle tree state in memory
- Processor and HTTP server share only DashMap (designed for concurrent access)
- No database contention from HTTP requests (memory-only operations)

### Phase 3: Universal Ordering System
**Goal**: Implement robust chronological ordering across multiple sources with different schemas

**Implementation Complete**:
- [x] Schema updates: Renamed order_column → id_column (must be unique per table)
- [x] Added optional timestamp_column to verification_sources
- [x] Universal ordering key: (Option<DateTime<Utc>>, i64, String)
- [x] Custom Ord implementation for SourceRow with explicit ordering logic:
  1. Timestamped entries sort before non-timestamped (Some < None)
  2. Within timestamped: chronological order by timestamp
  3. Secondary sort by source_id (unique per table)
  4. Tertiary sort by source_table (deterministic tie-breaker)
- [x] BTreeSet for O(log n) automatic sorted insertion
- [x] Conditional SQL queries based on timestamp_column presence
- [x] Test infrastructure for mixed timestamped/non-timestamped sources
- [x] Comprehensive ordering tests: test_universal_ordering and test_no_timestamp_ordering
- [x] Backward compatible: sources without timestamps gracefully degrade to ID-based ordering
- [x] All tests passing with verification of chronological and ID-based ordering

**Phase 3 Complete!** ✨

## 🚧 Next Steps

### Phase 4: Production Readiness

**Critical Issues**:

**Infrastructure Tasks**:
- [x] Error handling: What happens when source table doesn't exist or has wrong schema?
  - Implemented comprehensive validation module (validation.rs)
  - `--verify-db` flag for pre-deployment/operational checks with detailed reporting (cargo run --bin main -- --verify-db)
  - Startup validation warns about invalid sources without blocking service
  - Runtime resilience: skips invalid sources, continues processing valid ones
  - Validates table existence, column existence, and column types (id: bigint/integer, hash: bytea, timestamp: timestamp)
- [ ] Error handling: failed proofs vs leaf not yet present (add endpoint to check if leaf exists?)
- [ ] Monitoring: Metrics for processor lag, batch sizes, processing time per log
- [ ] Graceful shutdown: Stop processor cleanly, wait for in-flight batches
- [ ] Health check endpoint: /health with database connectivity check
- [ ] Log management API: Create/disable logs without database access
- [ ] Performance tuning: Configurable batch sizes and intervals per log
- [ ] Handle empty root case when merkle tree is empty (See routes::get_merkle_root), this root won't be in the root_hash_to_size map but it may have been returned from get_merkle_root
- [ ] ct-merkle expects data to be passed in which will be hashed, but we want to pass already computed hashes, the current implementation will be hashing our supplied hashes again
- [ ] investigate postgresql for merkle_log (partitions on the log_name seem a good idea)

**Documentation**:
- [ ] Deployment guide (environment variables, database setup)
- [ ] API documentation (OpenAPI/Swagger spec)
- [ ] Multi-log configuration examples
- [ ] Performance characteristics and tuning guide

**Testing**:
- [ ] Concurrent multi-log stress tests
- [ ] Error recovery scenarios (database disconnect, invalid source tables)
- [ ] Large dataset tests (millions of entries per log)

**Future Enhancements** (Post-Phase 4):
- [ ] Pruning old entries (if needed)
- [ ] Read replicas for HTTP layer
- [ ] Async proof generation for large trees
- [ ] Proof caching for frequently requested proofs

## 📝 Design Notes

**Current Architecture**:
- Multiple independent logs, each with their own merkle tree
- verification_logs table defines available logs (log_name, enabled flag)
- verification_sources maps source tables to logs (composite PK: source_table, log_name)
  - id_column (required): Must be unique per table for ordering guarantees
  - timestamp_column (optional): Enables chronological ordering
- merkle_log stores entries with log_name FK (CASCADE delete when log removed)
- DashMap<String, Arc<RwLock<MerkleState>>> provides concurrent per-log state
- Startup rebuild ensures in-memory trees match database before serving requests
- Batch processor: sole owner of all database operations (clean separation)
- HTTP handlers: memory-only operations, read from DashMap (fast, no DB contention)
- Sequential global merkle_log.id (SERIAL) - monotonic per log with gaps
- Universal ordering: (Option<Timestamp>, id, table_name) provides chronological ordering with graceful degradation

**Key Invariants**:
1. No gaps in merkle_log.id sequence **globally** (per-log may have gaps from other logs)
2. Tree only updated from committed merkle_log data
3. **merkle_log is ground truth and CANNOT be reconstructed deterministically from source tables**
   - Batch boundaries create different orderings depending on when batches run
   - Late-arriving entries with earlier timestamps cannot be retroactively inserted
   - Same source data produces different merkle roots depending on batching pattern
   - Therefore: merkle_log must be preserved and backed up for disaster recovery
4. Batch processor holds advisory lock per log during processing
5. All database writes happen in batch processor only
6. id_column must be unique per source table (typically primary key or unique constraint)

**Why merkle_log Cannot Be Rebuilt from Sources**:

Example demonstrating non-determinism:
- Scenario 1 (Two batches): 9 timestamped entries + X (no timestamp) → X at position 10
  Then 10 more timestamped entries in batch 2 → positions 11-20
  Result: X is at position 10

- Scenario 2 (One batch): Same 20 entries processed together
  All 19 timestamped entries come first → positions 1-19, X at position 20
  Result: X is at position 20

Same data, different merkle roots. This is correct behavior for append-only logs where ordering
is a point-in-time commitment based on what entries are known when the batch runs.

**Ordering Strategy**:
- Universal ordering key: (Option<DateTime<Utc>>, source_id, source_table)
- Timestamped sources: Chronological ordering by timestamp column (e.g., created_at)
- Non-timestamped sources: Sort after all timestamped entries, ordered by (id, table_name)
- Custom Ord implementation provides type-safe, compile-time verified ordering
- BTreeSet ensures O(log n) sorted insertion with automatic deduplication
- Conditional SQL queries: includes timestamp column only when configured
- Backward compatible: Optional timestamp column allows mixed timestamped/non-timestamped scenarios
- Deterministic and repeatable: Same database state always produces same tree