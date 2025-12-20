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

## 🚧 Next Steps

### Phase 2: Multiple Independent Logs
**Goal**: Support multiple independent merkle trees, each tracking different sets of source tables

**Design Questions**:
- [ ] How to identify which log a source belongs to? (log_name column? separate config table?)
- [ ] One merkle_state per log in AppState?
- [ ] Separate HTTP endpoints per log? (/logs/{log_name}/root, etc.)
- [ ] How to configure log definitions? (config file? database table?)

**Implementation Tasks**:
- [ ] Design configuration mechanism for log definitions
- [ ] Extend schema to support multiple logs
- [ ] Update batch processor to handle multiple independent logs
- [ ] Update routes to support log-scoped operations
- [ ] Add tests for multi-log scenarios

### Phase 3: Production Readiness

**Critical Issues**:
- [ ] **Fix multi-source ordering**: Current implementation sorts by source table ID, which is meaningless across tables
  - Problem: Table A with IDs 1-10,000 will always come after Table B with IDs 1-5
  - Solution: Make `SourceRow` generic over ordering key (e.g., `SourceRow<T: Ord>`)
  - Allow sorting by cross-table comparable values (timestamps, global sequence numbers)
  - Update SourceConfig to specify ordering column type/strategy
  - Keep same `sort_by_key` logic with generic ordering value

**Other Tasks**:
- [ ] Configuration file/database for source and log definitions
- [ ] Graceful shutdown handling
- [ ] Metrics/monitoring endpoints
- [ ] Performance tuning (batch sizes, intervals)
- [ ] Documentation for deployment
- [ ] Handle empty root case when merkle tree is empty (See routes::get_merkle_root)

## 📝 Design Notes

**Current Architecture**:
- Single merkle_log table stores entries from multiple source tables
- source_table + source_id provides unique identification
- Sequential merkle_log.id maintains global ordering
- In-memory merkle tree rebuilt on startup from merkle_log
- Batch processor runs every 1 second, processes up to 10k rows

**Key Invariants**:
1. No gaps in merkle_log.id sequence
2. Tree only updated from committed merkle_log data
3. merkle_log is ground truth (always rebuildable)
