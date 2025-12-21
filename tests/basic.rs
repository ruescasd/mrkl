// cargo test --test '*' -- --include-ignored
use anyhow::Result;
use base64::Engine;
use mrkl::service::Client;
use serial_test::serial;

async fn setup_client() -> Result<Client> {
    // Load environment variables from .env file
    dotenv::dotenv().ok();
    let client = Client::new("http://localhost:3000").await?;
    // Idempotent setup - safe to call multiple times
    client.setup_test_environment().await?;
    Ok(client)
}

async fn setup_client_multi_source() -> Result<Client> {
    dotenv::dotenv().ok();
    let client = Client::new_with_log("http://localhost:3000", "test_log_multi_source").await?;
    client.setup_test_environment().await?;
    Ok(client)
}

#[ignore]
#[serial]
#[tokio::test]
async fn test_inclusion_proofs() -> Result<()> {
    let client = setup_client().await?;

    use sha2::{Digest, Sha256};

    // Get initial log size (may not be 0 if tests ran before)
    let initial_size = client.get_log_size().await?;
    println!("📊 Initial log size: {}", initial_size);

    // Add several entries
    let entries = vec!["data1", "data2", "data3", "data4", "data5"];
    let mut hashes = Vec::new();

    // Add entries and collect their hashes
    for entry in &entries {
        let mut hasher = Sha256::new();
        hasher.update(entry.as_bytes());
        let hash = hasher.finalize();
        hashes.push(hash.to_vec());

        client.add_entry(entry).await?;
    }

    // Wait until all entries are processed
    let expected_size = initial_size + entries.len();
    println!("⏳ Waiting for log to reach size {}...", expected_size);
    match client.wait_until_log_size(expected_size).await {
        Ok(_) => {},
        Err(e) => {
            // Show diagnostic info on failure
            client.show_recent_entries(20).await?;
            return Err(e);
        }
    }

    // Get the current root
    let root = client.get_root().await?;

    // Verify inclusion proofs for all entries using their hashes
    for (entry, hash) in entries.iter().zip(hashes.iter()) {
        let proof = client.get_inclusion_proof(entry).await?;

        // Verify proof has correct root
        assert_eq!(
            proof.root, root,
            "Proof root should match current tree root for entry '{}'",
            entry
        );

        // Verify the proof itself
        assert!(
            proof.verify(hash)?,
            "Proof verification should succeed for entry '{}'",
            entry
        );

        println!(
            "✅ Verified inclusion proof for '{}' at index {}",
            entry, proof.index
        );
    }

    Ok(())
}

#[ignore]
#[serial]
#[tokio::test]
async fn test_consistency_proofs() -> Result<()> {
    let client = setup_client().await?;
    let mut historical_roots = Vec::new();

    // Track current size for efficient waiting
    let mut current_size = client.get_log_size().await?;
    println!("📊 Initial log size: {}", current_size);

    // Add entries and store intermediate roots
    for i in 0..3 {
        let mut batch_roots = Vec::new();
        for j in 0..3 {
            let entry = format!("batch{}-entry{}", i + 1, j + 1);

            // Add entry and compute its hash
            client.add_entry(&entry).await?;

            // Wait for this single entry to be processed
            current_size += 1;
            println!("⏳ Waiting for entry {}-{} (size {})...", i + 1, j + 1, current_size);
            client.wait_until_log_size(current_size).await?;
            
            // Store root after each entry
            let root = client.get_root().await?;
            println!(
                "📸 Storing intermediate root after entry {}-{}",
                i + 1,
                j + 1
            );
            batch_roots.push(root);
        }

        // Store the last root from this batch
        historical_roots.push(batch_roots.pop().unwrap());
        println!("📸 Storing final root for batch {}", i + 1);
    }

    // Add a final entry to change the tree state
    let entry = format!("last");
    client.add_entry(&entry).await?;
    current_size += 1;
    client.wait_until_log_size(current_size).await?;

    // Get final tree state
    let final_root = client.get_root().await?;

    // Verify consistency between each historical root and final state
    for (i, historical_root) in historical_roots.iter().enumerate() {
        println!("🔍 Verifying consistency with batch {}", i + 1);
        assert!(
            client
                .verify_tree_consistency(historical_root.clone())
                .await?,
            "Tree should be consistent with historical root from batch {}",
            i + 1
        );
    }

    println!("🔍 Consistency of root with itself should fail");
    assert!(
        client.verify_tree_consistency(final_root).await.is_err(),
        "Consistency of root with itself should fail"
    );

    Ok(())
}

#[ignore]
#[serial]
#[tokio::test]
async fn test_burst_operations() -> Result<()> {
    use sha2::{Digest, Sha256};

    let client = setup_client().await?;

    // Get initial log size
    let initial_size = client.get_log_size().await?;
    println!("📊 Initial log size: {}", initial_size);

    // Create a batch of entries and calculate their hashes
    let entries: Vec<String> = (1..=10).map(|i| format!("burst-entry-{}", i)).collect();

    let mut hashes = Vec::new();

    // Add all entries as quickly as possible
    println!("🌊 Starting burst of {} entries...", entries.len());
    for entry in &entries {
        // Calculate hash before adding
        let mut hasher = Sha256::new();
        hasher.update(entry.as_bytes());
        let hash = hasher.finalize();
        hashes.push(hash.to_vec());

        client.add_entry(entry).await?;
    }

    // Wait until all entries are processed
    let expected_size = initial_size + entries.len();
    println!("⏳ Waiting for log to reach size {}...", expected_size);
    client.wait_until_log_size(expected_size).await?;

    // Get final root
    let root = client.get_root().await?;
    println!(
        "🌳 Final merkle root after burst: {}",
        base64::engine::general_purpose::STANDARD.encode(&root)
    );

    // Verify all entries are properly included using their hashes
    for (entry, hash) in entries.iter().zip(hashes.iter()) {
        let proof = client.get_inclusion_proof(entry).await?;
        assert_eq!(
            root, proof.root,
            "Merkle root mismatch for entry: {}",
            entry
        );
        assert!(
            proof.verify(hash)?,
            "Proof verification failed for entry: {}",
            entry
        );
        println!("✅ Verified proof for '{}' at index {}", entry, proof.index);
    }

    Ok(())
}

#[ignore]
#[serial]
#[tokio::test]
async fn test_large_batch_performance() -> Result<()> {
    use sha2::{Digest, Sha256};

    let client = setup_client().await?;
    let num_entries = 10_000;

    // Get initial log size
    let initial_size = client.get_log_size().await?;
    println!("📊 Initial log size: {}", initial_size);

    println!(
        "🔥 Starting performance test with {} entries...",
        num_entries
    );

    // Add entries in chunks to avoid overwhelming the system
    let chunk_size = 1000;
    let mut all_hashes = Vec::with_capacity(num_entries);

    for chunk_start in (0..num_entries).step_by(chunk_size) {
        let chunk_end = (chunk_start + chunk_size).min(num_entries);
        println!("📦 Adding entries {}-{}", chunk_start, chunk_end);

        let chunk_start_time = std::time::Instant::now();
        let mut chunk_hashes = Vec::with_capacity(chunk_size);

        // Add entries and calculate hashes in this chunk
        for i in chunk_start..chunk_end {
            let entry = format!("perf-entry-{}", i);

            // Calculate hash
            let mut hasher = Sha256::new();
            hasher.update(entry.as_bytes());
            let hash = hasher.finalize();

            // Add entry to system first
            client.add_entry(&entry).await?;

            // Store entry and hash for later verification
            chunk_hashes.push((entry, hash.to_vec()));
        }

        // Extend our total hash collection
        all_hashes.extend(chunk_hashes);

        let chunk_duration = chunk_start_time.elapsed();
        println!("  ⏱️ Chunk insert time: {:?}", chunk_duration);
        println!(
            "  📊 Average insert time: {:?} per entry",
            chunk_duration / chunk_size as u32
        );
    }

    // Wait for final processing and get root
    let expected_size = initial_size + num_entries;
    println!("\n⏳ Waiting for log to reach size {}...", expected_size);
    client.wait_until_log_size(expected_size).await?;

    // Get and verify final root
    let root = client.get_root().await?;

    println!(
        "Final root: {}",
        base64::engine::general_purpose::STANDARD.encode(&root)
    );

    // Verify some sample entries
    println!("\n🔍 Verifying sample entries...");
    let sample_indices = vec![
        0,
        num_entries / 4,
        num_entries / 2,
        (3 * num_entries) / 4,
        num_entries - 1,
    ];

    for idx in sample_indices {
        let (entry, hash) = &all_hashes[idx];
        let proof = client.get_inclusion_proof(entry).await?;
        assert!(
            proof.verify(hash)?,
            "Proof verification failed for entry: {}",
            entry
        );
        println!("✅ Verified entry at index {}", idx);
    }

    Ok(())
}

#[ignore]
#[serial]
#[tokio::test]
async fn test_source_log_setup() -> Result<()> {
    // Use the multi-source log for this test
    let client = setup_client_multi_source().await?;

    // Get initial size
    let initial_size = client.get_log_size().await?;
    println!("📊 Initial log size: {}", initial_size);

    // Add entries to all three sources with interleaved timing
    println!("\n📝 Adding entries to multiple sources...");
    client.add_entry_to_source("source_log", "entry-s-1").await?;
    client.add_entry_to_source("test_source_a", "entry-a-1").await?;
    client.add_entry_to_source("test_source_b", "entry-b-1").await?;
    client.add_entry_to_source("source_log", "entry-s-2").await?;
    client.add_entry_to_source("test_source_a", "entry-a-2").await?;
    println!("✅ Added 5 entries across 3 source tables");

    // Wait for batch processor to process all entries
    let expected_size = initial_size + 5;
    println!("\n⏳ Waiting for log to reach size {}...", expected_size);
    client.wait_until_log_size(expected_size).await?;

    // Verify entries were processed into merkle_log from all sources
    let rows = client.get_sources().await?;

    assert!(rows.len() >= 5, "Should have processed at least 5 entries");
    
    // Verify merkle_log IDs are strictly increasing (monotonic)
    // They won't be sequential (1,2,3...) because the ID sequence is shared across all logs
    for i in 1..rows.len() {
        let prev_id: i64 = rows[i-1].get(2);
        let curr_id: i64 = rows[i].get(2);
        assert!(
            curr_id > prev_id,
            "Merkle log IDs should be strictly increasing: {} should be > {}",
            curr_id, prev_id
        );
    }
    
    println!("\n✅ Multi-source test complete!");
    println!("   - {} total entries in merkle_log", rows.len());
    println!("   - Entries from {} different sources", 
        rows.iter()
            .map(|r| r.get::<_, String>(0))
            .collect::<std::collections::HashSet<_>>()
            .len()
    );
    println!("   - Merkle log IDs are strictly increasing");
    println!("   - Phase 1 validation: SUCCESS! ✨");
    
    Ok(())
}
