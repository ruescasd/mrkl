# Merkle Tree PostgreSQL Integration - TODO List

## Project Overview
This project implements a real-time Merkle tree that tracks PostgreSQL table updates using LISTEN/NOTIFY, providing cryptographic proofs of data inclusion. The system maintains an in-memory Merkle tree that stays synchronized with the database, enabling proof generation without modifying the underlying database.

## Current Status
- ✅ Basic Merkle tree implementation with ct-merkle
- ✅ PostgreSQL LISTEN/NOTIFY integration
- ✅ Real-time tree updates
- ✅ Basic inclusion proof generation
- ✅ Tree rebuild functionality

## Planned Improvements

### 1. Optimize Tree Entry Lookup
- [ ] Replace linear search with HashMap<String, usize>
- [ ] Add HashMap to track data -> index mapping
- [ ] Update push operations to maintain the mapping
- [ ] Update get_inclusion_proof to use the map
- [ ] Consider persistence strategy for the mapping

### 2. Implement HashableLeaf Optimization
- [ ] Create custom type wrapping data and its hash (using existing leaf_hash column)
- [ ] Implement HashableLeaf trait for the custom type
- [ ] Modify tree operations to use the existing leaf_hash values
- [ ] Update fetch_all_entries and notification handler to use HashedEntry type
- [ ] Add validation to ensure hash consistency

### 3. Improve Error Handling
- [ ] Create custom Error enum for the application
- [ ] Add proper error context and chaining
- [ ] Replace println! with proper logging
- [ ] Add error type conversions (From implementations)
- [ ] Add error recovery strategies

### 4. Add Proof Verification Endpoint
- [ ] Add ProofVerification struct for request parsing
- [ ] Implement /verify endpoint
- [ ] Add documentation with example requests
- [ ] Include test cases
- [ ] Add rate limiting

### 5. Add Proof Documentation
- [ ] Document proof generation API
- [ ] Document proof verification (API and manual)
- [ ] Provide example code in multiple languages
- [ ] Document security considerations
- [ ] Add OpenAPI/Swagger documentation

### 6. Implement Consistency Proofs
- [ ] Add consistency proof generation endpoint
- [ ] Add consistency proof verification
- [ ] Update documentation
- [ ] Add examples of proving tree state transitions

## Technical Notes

### Hashmap Implementation Considerations
- Need to handle initial population during rebuild
- Consider memory usage vs. lookup time tradeoff
- May need periodic cleanup for very large datasets

### HashableLeaf Implementation
```rust
pub struct HashedEntry {
    data: String,
    hash: [u8; 32],  // Pre-computed SHA-256 hash
}

impl HashableLeaf for HashedEntry {
    fn as_bytes(&self) -> &[u8] {
        &self.hash
    }
}
```

### Error Handling Strategy
```rust
#[derive(Error, Debug)]
pub enum AppError {
    #[error("Database error: {0}")]
    Database(#[from] tokio_postgres::Error),
    
    #[error("Tree error: {0}")]
    Tree(String),
    
    #[error("Proof verification failed: {0}")]
    ProofVerification(String),
    
    #[error("Invalid request: {0}")]
    InvalidRequest(String),
}
```

## Future Considerations
- Performance benchmarking and optimization
- High availability setup with replicas
- Backup and recovery procedures
- Monitoring and alerting
- Integration with popular blockchain networks
- Support for range proofs
- Batch proof generation
- Proof aggregation