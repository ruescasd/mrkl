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

## 🚧 Next Steps

### Phase 3: Production Readiness

**Critical Issues**:
- [ ] **Fix multi-source ordering**: Current implementation sorts by source table ID, which is meaningless across tables
  - Problem: Table A with IDs 1-10,000 will always come after Table B with IDs 1-5
  - Current: Uses `order_value` from source table's order column (e.g., id, created_at)
  - Issue: Different tables may use different value ranges/types
  - Solution options:
    1. Require all source tables to use same ordering scheme (e.g., all timestamps)
    2. Make ordering column configurable per-source with type information
    3. Use composite sort key: (order_value, source_table, source_id) - current implementation
  - **Current implementation uses composite key which provides deterministic ordering**
  - Need to validate: Does this meet real-world requirements? Or do we need timestamp-based ordering?

**Infrastructure Tasks**:
- [ ] Error handling: What happens when source table doesn't exist or has wrong schema?
- [ ] Monitoring: Metrics for processor lag, batch sizes, processing time per log
- [ ] Graceful shutdown: Stop processor cleanly, wait for in-flight batches
- [ ] Health check endpoint: /health with database connectivity check
- [ ] Log management API: Create/disable logs without database access
- [ ] Performance tuning: Configurable batch sizes and intervals per log
- [ ] Handle empty root case when merkle tree is empty (See routes::get_merkle_root)

**Documentation**:
- [ ] Deployment guide (environment variables, database setup)
- [ ] API documentation (OpenAPI/Swagger spec)
- [ ] Multi-log configuration examples
- [ ] Performance characteristics and tuning guide

**Testing**:
- [ ] Concurrent multi-log stress tests
- [ ] Error recovery scenarios (database disconnect, invalid source tables)
- [ ] Large dataset tests (millions of entries per log)

**Future Enhancements** (Post-Phase 3):
- [ ] Pruning old entries (if needed)
- [ ] Read replicas for HTTP layer
- [ ] Async proof generation for large trees
- [ ] Proof caching for frequently requested proofs

## 📝 Design Notes

**Current Architecture**:
- Multiple independent logs, each with their own merkle tree
- verification_logs table defines available logs (log_name, enabled flag)
- verification_sources maps source tables to logs (composite PK: source_table, log_name)
- merkle_log stores entries with log_name FK (CASCADE delete when log removed)
- DashMap<String, Arc<RwLock<MerkleState>>> provides concurrent per-log state
- Startup rebuild ensures in-memory trees match database before serving requests
- Batch processor: sole owner of all database operations (clean separation)
- HTTP handlers: memory-only operations, read from DashMap (fast, no DB contention)
- Sequential global merkle_log.id (SERIAL) - monotonic per log with gaps

**Key Invariants**:
1. No gaps in merkle_log.id sequence **globally** (per-log may have gaps from other logs)
2. Tree only updated from committed merkle_log data
3. merkle_log is ground truth (always rebuildable)
4. Batch processor holds advisory lock per log during processing
5. All database writes happen in batch processor only

**Ordering Strategy**:
- Composite sort key: (order_value, source_table, source_id)
- order_value: value from source table's order column (configurable per source)
- source_table: ensures deterministic ordering when order_values collide
- source_id: tie-breaker within same source table
- This provides deterministic, repeatable ordering across multiple sources
- Limitation: May not match wall-clock time if sources use different ordering schemes
