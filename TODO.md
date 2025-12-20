# Merkle Tree PostgreSQL Integration - TODO List

## Project Overview
This project implements a verifiable log using PostgreSQL, providing cryptographic proofs of data inclusion and ordering. The system uses batch processing with strong ordering guarantees to maintain a consistent Merkle tree state, enabling reliable proof generation while ensuring data consistency.

## Current Status
- ✅ Robust batch processing with strict ordering
- ✅ Two-table design with strong consistency guarantees
- ✅ High-performance batch inserts (~575K rows/sec)
- ✅ Gap-free sequential processing
- ✅ Basic Merkle tree implementation with ct-merkle
- ✅ Advisory locks for concurrency control
- ✅ Full ACID compliance and durability

## Planned Improvements

### 1. Multi-Source Verification System
- [ ] Add source table identifier to processed_log
- [ ] Implement generic source table interface
- [ ] Add support for multiple concurrent source tables
- [ ] Create example with multiple event sources

### 2. Operational Improvements
- [ ] Implement proper error handling and recovery
- [ ] Add structured logging
- [ ] Create monitoring dashboards
- [ ] Add performance metrics collection
- [ ] Implement health checks

### Multi-Source Design Considerations
- Source tables need only:
  - Ordering guarantee (timestamp or sequence)
  - Hash computation capability
- processed_log focuses on verification:
  - Sequential IDs
  - Source reference
  - Hash storage
  - No data duplication