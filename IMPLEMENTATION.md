# Service Modification Plan

## 1. State Changes
- Remove `next_id` from `AppState` (no longer needed as processed_log guarantees order)
- Remove notification channel and handler

## 2. Processing Loop
Replace notification handler with periodic batch processor:
```rust
async fn process_batches(state: AppState, interval: Duration) {
    loop {
        // Process next batch
        match state.client.query_one(
            "SELECT * FROM process_next_batch($1)", 
            &[&1000]
        ).await {
            Ok(row) => {
                let rows_processed: i64 = row.get("rows_processed");
                if rows_processed > 0 {
                    let first_id: i64 = row.get("first_id");
                    let last_id: i64 = row.get("last_id");
                    
                    // Fetch and process the new rows
                    let rows = state.client.query(
                        "SELECT id, data, leaf_hash 
                         FROM processed_log 
                         WHERE id BETWEEN $1 AND $2 
                         ORDER BY id",
                        &[&first_id, &last_id]
                    ).await?;

                    for row in rows {
                        let hash: Vec<u8> = row.get("leaf_hash");
                        let data: String = row.get("data");
                        process_row(&state, &data, &hash);
                    }
                    
                    println!("✅ Processed {} new rows", rows_processed);
                }
            }
            Err(e) => {
                println!("⚠️ Error processing batch: {}", e);
            }
        }
        
        // Wait for next interval
        tokio::time::sleep(interval).await;
    }
}

fn process_row(state: &AppState, data: &str, hash: &[u8]) {
    let leaf_hash = LeafHash::new(hash.to_vec());
    
    // Update the merkle tree, index map, and root map
    let mut tree = state.merkle_tree.write();
    let idx = tree.len();
    tree.push(leaf_hash);
    state.index_map.insert(data.to_string(), idx as usize);
    let current_root = tree.root();
    state.root_map.insert(current_root.as_bytes().to_vec(), tree.len() as usize);
    
    println!("📄 New entry added:");
    println!("   Data: {}", data);
    println!("   Current merkle root: {:?}", current_root.as_bytes());
    println!("   Tree size: {}", tree.len());
    println!("---");
}
```

## 3. Tree Rebuild
Modify `rebuild_tree` to use `processed_log`:
```rust
pub async fn rebuild_tree(state: &AppState) -> anyhow::Result<(usize, Vec<u8>)> {
    println!("🔄 Starting tree rebuild...");
    
    // Fetch all entries from processed_log (guaranteed to be in order)
    let rows = state.client
        .query("SELECT data, leaf_hash FROM processed_log ORDER BY id", &[])
        .await?;
    
    // Clear and rebuild tree
    state.index_map.clear();
    state.root_map.clear();
    let mut tree = state.merkle_tree.write();
    *tree = MemoryBackedTree::new();
    
    // Process rows in order
    for (idx, row) in rows.iter().enumerate() {
        let data: String = row.get("data");
        let hash: Vec<u8> = row.get("leaf_hash");
        let leaf_hash = LeafHash::new(hash);
        
        tree.push(leaf_hash);
        state.index_map.insert(data, idx);
        let current_root = tree.root();
        state.root_map.insert(current_root.as_bytes().to_vec(), tree.len() as usize);
    }
    
    let size = tree.len();
    let root = tree.root().as_bytes().to_vec();
    
    Ok((size, root))
}
```

## 4. Client Changes
Update `add_entry` in client.rs to just insert into `append_only_log`:
```rust
pub async fn add_entry(&self, data: &str) -> Result<i32> {
    let hash = compute_leaf_hash(data);
    let row = self.db_client
        .query_one(
            "INSERT INTO append_only_log (data, leaf_hash) VALUES ($1, $2) RETURNING id",
            &[&data, &hash],
        )
        .await?;
    Ok(row.get(0))
}
```

## Benefits
1. No more race conditions or ordering concerns
2. Simpler, more robust processing
3. Easy to monitor and validate
4. Clear separation between input and processing
5. Guaranteed consistency between rebuilds

Would you like me to start implementing these changes?