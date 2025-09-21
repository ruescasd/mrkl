# Simplified Design: Serial Processing with Strong Ordering Guarantees

## Core Insight
Instead of dealing with concurrent writes and notification ordering, we can create a strictly serial process using PostgreSQL's strong isolation capabilities.

## Design Overview

### 1. Two-Table Design

#### Source Table (`append_only_log`)
- Remains as is
- Allows concurrent writes
- No special constraints
- Functions as a staging area

#### Processing Table (`processed_log`)
- Strictly controlled
- Single writer (our processing function)
- Guaranteed sequential IDs
- No gaps allowed
- Immutable once written

### 2. Processing Function

```sql
CREATE OR REPLACE FUNCTION process_next_batch()
RETURNS void AS $$
DECLARE
    last_processed_id bigint;
BEGIN
    -- Get exclusive lock to ensure serial execution
    PERFORM pg_advisory_xact_lock(hashtext('process_next_batch'));
    
    -- Find last processed ID
    SELECT COALESCE(MAX(id), 0) INTO last_processed_id 
    FROM processed_log;
    
    -- Insert new entries in strict order
    INSERT INTO processed_log (id, data, leaf_hash)
    SELECT 
        ROW_NUMBER() OVER () + last_processed_id,
        data,
        leaf_hash
    FROM append_only_log al
    WHERE NOT EXISTS (
        SELECT 1 FROM processed_log pl 
        WHERE pl.data = al.data
    )
    ORDER BY al.id;
    
END;
$$ LANGUAGE plpgsql;
```

### 3. Service Design

```rust
async fn periodic_update(interval: Duration) {
    loop {
        // 1. Call processing function
        client.execute("SELECT process_next_batch()", &[]).await?;
        
        // 2. Read all new entries from processed_log
        let new_entries = client
            .query(
                "SELECT id, data, leaf_hash 
                 FROM processed_log 
                 WHERE id > $1 
                 ORDER BY id",
                &[&last_processed_id]
            )
            .await?;
            
        // 3. Update Merkle tree with new entries
        // (guaranteed to be in correct order)
        for entry in new_entries {
            update_merkle_tree(entry);
        }
        
        // 4. Wait for next interval
        tokio::time::sleep(interval).await;
    }
}
```

## Advantages

1. **Simplification**
   - Eliminates notification system
   - Removes need for buffering out-of-order messages
   - No gap handling required
   - No concurrent write concerns

2. **Strong Guarantees**
   - Sequential IDs guaranteed by PostgreSQL
   - No possibility of gaps
   - Immutable history
   - Single source of truth

3. **Clear Consistency Properties**
   - Tree always reflects exact state of `processed_log`
   - `processed_log` has strict ordering
   - Rebuilds will always produce identical trees

4. **Operational Benefits**
   - Easier to monitor
   - Simpler failure recovery
   - Clear progress tracking
   - Natural batching of updates

## Implementation Notes

1. **Advisory Locks**
   - Use PostgreSQL advisory locks for strict serialization
   - Ensures only one batch process runs at a time
   - Handles concurrent service instances safely

2. **Batching**
   - Natural batching of updates reduces system load
   - Can tune interval based on requirements
   - Trade-off between latency and efficiency

3. **Recovery**
   - Simple recovery by reading from last known ID
   - No complex state to maintain
   - Can rebuild from scratch trivially

## Trade-offs

1. **Latency**
   - Higher latency for tree updates
   - Updates happen in batches rather than real-time
   - Configurable based on requirements

2. **Throughput**
   - Source table can handle high concurrent writes
   - Processing is batched and sequential
   - May not suit real-time requirements

3. **Storage**
   - Requires two copies of data
   - Simple to prune source table after processing
   - Processed table becomes authoritative

## Conclusion

This design dramatically simplifies the system by leveraging PostgreSQL's strong isolation capabilities to create an immutable, ordered source of truth. It trades some latency for strong consistency guarantees and operational simplicity.