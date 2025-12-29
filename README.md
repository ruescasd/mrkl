# mrkl

**PostgreSQL-backed Merkle Tree Verification Service for Certificate Transparency**

A high-performance Rust service that maintains cryptographically verifiable Merkle trees over PostgreSQL data. Designed for Certificate Transparency log monitoring and general-purpose append-only log verification.

## Features

- **Multiple Independent Logs** - Run multiple Merkle trees simultaneously, each tracking different data sources
- **Universal Ordering** - Chronological ordering across heterogeneous source tables with optional timestamp-based merging
- **High Performance** - Processes 150,000+ rows/second with multi-row INSERT optimization
- **Production Ready** - Comprehensive error handling, graceful shutdown, and runtime validation
- **Zero-Downtime Operations** - Pause/resume batch processing without stopping the HTTP API
- **Real-Time Monitoring** - TUI dashboard and JSON metrics endpoint for observability
- **Cryptographic Proofs** - Full support for inclusion and consistency proofs with verification
- **Architectural Guarantees** - Clean separation between batch processor (DB owner) and HTTP server (memory-only)

## Quick Start

### Prerequisites

- Rust 1.83+ (edition 2024)
- PostgreSQL 12+
- Environment variables in `.env`:
  ```bash
  DATABASE_URL=postgres://user:password@localhost/dbname
  MRKL_SERVER_URL=http://localhost:3000  # Optional, for client examples
  ```

### Installation

```bash
# Clone and build
git clone https://github.com/yourusername/mrkl.git
cd mrkl
cargo build --release

# Initialize database schema
cargo run --bin setup --release

# Run the server
cargo run --bin main --release
```

### Basic Usage

The server starts on `http://localhost:3000` with two components:
- **Batch Processor** - Continuously monitors source tables and updates Merkle trees
- **HTTP API** - Serves proofs and tree state queries (memory-only, no DB contention)

#### 1. Configure a Log

```sql
-- Create a log
INSERT INTO verification_logs (log_name, description, enabled)
VALUES ('my_log', 'My verification log', true);

-- Register a source table
INSERT INTO verification_sources 
  (source_table, log_name, hash_column, id_column, timestamp_column, enabled)
VALUES 
  ('certificates', 'my_log', 'leaf_hash', 'id', 'created_at', true);
```

Your source table must have:
- `id_column` - Unique identifier (typically `BIGSERIAL PRIMARY KEY`)
- `hash_column` - Pre-computed SHA256 hash (`BYTEA NOT NULL`)
- `timestamp_column` - Optional timestamp for chronological ordering (`TIMESTAMPTZ`)

#### 2. Verify Inclusion

```rust
use mrkl::service::Client;

let client = Client::new("http://localhost:3000")?;

// Get inclusion proof for some data
let proof = client.get_inclusion_proof("my_log", b"my data").await?;

// Verify it cryptographically
let hash = sha256(b"my data");
proof.verify(&hash)?;
```

#### 3. Monitor Consistency

```rust
// Get current root
let current = client.get_root("my_log").await?;

// Later, verify the log grew consistently
let proof = client.get_consistency_proof("my_log", &old_root).await?;
proof.verify(&old_root, &current.root)?;
```

See [examples/post.rs](examples/post.rs) for a complete workflow.

## Architecture

### Ground Truth Invariant

**⚠️ CRITICAL**: The `merkle_log` table is ground truth and **cannot be reconstructed** deterministically from source tables.

**Why?** Batch boundaries and late arrivals create path-dependent ordering:

```
Scenario 1 (Two batches):
  Batch 1: [A(t=1), B(t=2), C(no timestamp)] → C at position 3
  Batch 2: [D(t=3), E(t=4)]                  → positions 4-5
  
Scenario 2 (One batch):
  Batch 1: [A(t=1), B(t=2), D(t=3), E(t=4), C(no timestamp)]
  Result: C at position 5 (after all timestamped entries)
```

Same source data → different Merkle roots. This is **correct behavior** for append-only transparency logs where ordering is a point-in-time commitment.

**Implications**:
- ✅ Startup rebuild from `merkle_log` is deterministic
- ❌ Rebuild from source tables is NOT deterministic
- 🔒 `merkle_log` must be backed up for disaster recovery

### Component Separation

```
┌─────────────────────────────────────────┐
│         Batch Processor (Tokio Task)    │
│  - Sole owner of all database writes    │
│  - Queries source tables every 1s       │
│  - Inserts into merkle_log              │
│  - Updates in-memory trees              │
└──────────────┬──────────────────────────┘
               │ Updates
               ▼
     ┌──────────────────────┐
     │  DashMap<String,     │
     │    Arc<RwLock<       │
     │      MerkleState>>>  │
     └──────────┬───────────┘
                │ Reads
                ▼
┌─────────────────────────────────────────┐
│         HTTP Server (Axum)              │
│  - Memory-only operations               │
│  - Never touches database               │
│  - Fast, lock-free reads via DashMap    │
└─────────────────────────────────────────┘
```

## Configuration

### Database Schema

The service uses three core tables:

#### `verification_logs`
Defines available logs:
```sql
CREATE TABLE verification_logs (
    log_name    TEXT PRIMARY KEY,
    created_at  TIMESTAMP DEFAULT NOW(),
    description TEXT,
    enabled     BOOLEAN DEFAULT true
);
```

#### `verification_sources`
Maps source tables to logs:
```sql
CREATE TABLE verification_sources (
    source_table     TEXT NOT NULL,
    log_name         TEXT NOT NULL REFERENCES verification_logs(log_name) ON DELETE CASCADE,
    hash_column      TEXT NOT NULL,
    id_column        TEXT NOT NULL,
    timestamp_column TEXT,  -- Optional, enables chronological ordering
    enabled          BOOLEAN DEFAULT true,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (source_table, log_name)
);
```

#### `merkle_log`
Ground truth storage (managed by batch processor):
```sql
CREATE TABLE merkle_log (
    id              BIGSERIAL PRIMARY KEY,  -- Global sequence
    log_name        TEXT NOT NULL REFERENCES verification_logs(log_name) ON DELETE RESTRICT,
    source_table    TEXT NOT NULL,
    source_id       BIGINT NOT NULL,
    leaf_hash       BYTEA NOT NULL,
    processed_at    TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (log_name, source_table, source_id)
);
```

### Universal Ordering

Entries are ordered by: `(Option<Timestamp>, source_id, source_table)`

- **With timestamp_column**: Chronological order across all sources
- **Without timestamp_column**: ID-based order, sorts after timestamped entries
- Deterministic tie-breaking via `(source_id, source_table)` tuple

### Runtime Validation

Check your configuration before deployment:

```bash
cargo run --bin main -- --verify-db
```

Validates:
- Table existence
- Column existence and types
- Proper schema alignment

## API Reference

All endpoints return JSON with `"status": "ok"` or `"status": "error"`.

### Tree State

#### `GET /logs/{log_name}/root`
Current Merkle root.

**Response:**
```json
{
  "status": "ok",
  "log_name": "my_log",
  "root": "base64-encoded-root",
  "tree_size": 12345
}
```

**Errors**: `LogNotFound`, `EmptyTree`

#### `GET /logs/{log_name}/size`
Current tree size.

**Response:**
```json
{
  "status": "ok",
  "log_name": "my_log",
  "tree_size": 12345
}
```

### Proofs

#### `GET /logs/{log_name}/proof?hash=<base64>`
Inclusion proof for a leaf hash.

**Response:**
```json
{
  "status": "ok",
  "log_name": "my_log",
  "index": 42,
  "tree_size": 100,
  "root": "base64-root",
  "proof": ["base64-hash1", "base64-hash2"]
}
```

**Errors**: `LogNotFound`, `EmptyTree`, `ProofGenerationFailed`

#### `GET /logs/{log_name}/consistency?old_root=<base64>`
Consistency proof between old root and current root.

**Response:**
```json
{
  "status": "ok",
  "log_name": "my_log",
  "old_root": "base64-old-root",
  "new_root": "base64-new-root",
  "old_size": 50,
  "new_size": 100,
  "proof": ["base64-hash1", "base64-hash2"]
}
```

**Errors**: `LogNotFound`, `EmptyTree`, `ProofGenerationFailed`

### Queries

#### `GET /logs/{log_name}/has_leaf?hash=<base64>`
Check if a leaf exists (O(1) via index map).

**Response:**
```json
{
  "status": "ok",
  "log_name": "my_log",
  "exists": true
}
```

#### `GET /logs/{log_name}/has_root?root=<base64>`
Check if a root exists in history (O(1) via root map).

**Response:**
```json
{
  "status": "ok",
  "log_name": "my_log",
  "exists": true
}
```

### Admin

#### `POST /admin/pause`
Pause batch processing (HTTP server continues).

#### `POST /admin/resume`
Resume batch processing.

#### `POST /admin/stop`
Gracefully stop batch processor.

#### `GET /admin/status`
Check processor state: `Running`, `Paused`, or `Stopped`.

### Monitoring

#### `GET /metrics`
Detailed performance metrics (JSON).

**Response includes:**
- Per-log metrics: rows copied, timing breakdown (query/insert/fetch/tree)
- Global metrics: cycle duration, active logs, idle percentage

## Monitoring

### TUI Dashboard

Real-time monitoring with the terminal UI:

```bash
cargo run --bin dashboard --release
```

Shows:
- Per-log statistics (throughput, latency, tree size)
- Global system health (cycle time, idle %)
- Live updates every second

### Load Testing

Simulate high-throughput workloads:

```bash
cargo run --bin load --release -- \
  --rows-per-interval 1000 \
  --num-sources 3
```

### Metrics Endpoint

Query metrics programmatically:

```bash
curl http://localhost:3000/metrics | jq
```

## Performance

Current benchmarks (measured with load generator):

- **Throughput**: 150,000+ rows/second
- **Latency**: 12-15ms per 1000-row batch (query + insert + tree update)
- **Idle Time**: 94% (system is rarely bottlenecked)

**Timing Breakdown** (per batch):
- Query source tables: ~5-7ms
- Insert into merkle_log: ~5-7ms (multi-row INSERT optimization)
- Tree updates: 0-1ms (RwLock not a bottleneck)

**Optimizations Applied**:
- Multi-row INSERT (10x improvement over single-row)
- Advisory locks per log (no cross-log contention)
- Memory-only HTTP operations (no DB queries)
- Early-exit source table queries (stops at batch_size)

## Development

### Building

```bash
# Debug build
cargo build

# Release build (optimized)
cargo build --release

# With specific features
cargo build --features "serde"
```

### Testing

```bash
# Run all tests
cargo test

# Run with output
cargo test -- --nocapture

# Run specific test
cargo test test_universal_ordering

# Integration tests only
cargo test --test '*'
```

Tests use `serial_test` to prevent concurrent database access.

### Linting

Strict lint configuration enforced:

```bash
# Check all lints
cargo clippy --all-targets -- -D warnings

# Fix auto-fixable lints
cargo clippy --fix --all-targets
```

Active lint groups:
- `clippy::pedantic` (library and main.rs only)
- `clippy::suspicious`, `clippy::complexity`, `clippy::style`, `clippy::perf`
- Restriction lints: `unwrap_used`, `panic`, `arithmetic_side_effects`, `indexing_slicing`, `print_stdout`, `print_stderr`

### Examples

```bash
# Post an entry and verify inclusion
cargo run --example post

# Monitor consistency (CT-style auditor)
cargo run --example monitor
```

## Roadmap

See [TODO.md](TODO.md) for detailed task tracking.

**Completed:**
- ✅ Multiple independent logs with per-log configuration
- ✅ Universal ordering system (chronological + ID-based)
- ✅ Comprehensive error handling and validation
- ✅ Performance monitoring and optimization
- ✅ Graceful shutdown and pause control

**Next Steps:**
- [ ] Deployment guide (Docker, systemd)
- [ ] OpenAPI/Swagger specification
- [ ] Proof caching for frequently requested proofs
- [ ] Read replicas for HTTP layer scaling

## License

[Your License Here]

## Contributing

[Your Contributing Guidelines Here]

## Acknowledgments

Built with:
- [ct-merkle](https://crates.io/crates/ct-merkle) - RFC 6962 Merkle tree implementation
- [Axum](https://crates.io/crates/axum) - Web framework
- [deadpool-postgres](https://crates.io/crates/deadpool-postgres) - Connection pooling
- [tokio](https://crates.io/crates/tokio) - Async runtime
