// cargo test --test '*' -- --include-ignored
use anyhow::Result;
use base64::Engine;
use mrkl::service::Client;
use serial_test::serial;

async fn setup_client() -> Result<Client> {
    // Load environment variables from .env file
    dotenv::dotenv().ok();
    Client::new("http://localhost:3000").await
}

#[ignore]
#[serial]
#[tokio::test]
async fn test_inclusion_proofs() -> Result<()> {

    let client = setup_client().await?;
    client.setup_and_register_sources(&["source_log"]).await?;

    use sha2::{Digest, Sha256};

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

    // Wait for all entries to be processed
    println!("Waiting for entries to be processed...");
    client.wait_for_processing().await?;
    println!("✅ Sleep complete");

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
    client.setup_and_register_sources(&["source_log"]).await?;
    let mut historical_roots = Vec::new();

    // Add entries and store intermediate roots
    for i in 0..3 {
        let mut batch_roots = Vec::new();
        for j in 0..3 {
            let entry = format!("batch{}-entry{}", i + 1, j + 1);

            // Add entry and compute its hash
            client.add_entry(&entry).await?;

            // Wait for this single entry to be processed
            println!("Waiting for entry {}-{} to be processed...", i + 1, j + 1);
            client.wait_for_processing().await?;
            
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
    client.wait_for_processing().await?;

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
    client.setup_and_register_sources(&["source_log"]).await?;

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

    // Give the system a moment to process all entries
    client.wait_for_processing().await?;

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
    client.setup_and_register_sources(&["source_log"]).await?;
    let num_entries = 10_000;

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
    println!("\n⏳ Waiting for all entries to be processed...");
    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

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

    // Create and register all test sources
    let all_sources = ["source_log", "test_source_a", "test_source_b"];
    let client = setup_client().await?;
    client.setup_and_register_sources(&all_sources).await?;

    // Add entries to all three sources with interleaved timing
    println!("\n📝 Adding entries to multiple sources...");
    client.add_entry_to_source("source_log", "entry-s-1").await?;
    client.add_entry_to_source("test_source_a", "entry-a-1").await?;
    client.add_entry_to_source("test_source_b", "entry-b-1").await?;
    client.add_entry_to_source("source_log", "entry-s-2").await?;
    client.add_entry_to_source("test_source_a", "entry-a-2").await?;
    println!("✅ Added 5 entries across 3 source tables");

    // Wait for batch processor
    println!("\n⏳ Waiting for batch processor (3 seconds)...");
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Verify entries were processed into merkle_log from all sources
    let rows = client.get_sources().await?;

    println!("\n📊 Processed entries in merkle_log:");
    for row in &rows {
        let source_table: String = row.get(0);
        let source_id: i64 = row.get(1);
        let merkle_id: i64 = row.get(2);
        println!("  merkle_id={}, source={}:{}", merkle_id, source_table, source_id);
    }

    assert!(rows.len() >= 5, "Should have processed at least 5 entries");
    
    // Verify sequential merkle_log IDs
    let expected_start = if rows.len() > 5 { rows.len() - 5 } else { 0 };
    for (idx, row) in rows.iter().skip(expected_start).enumerate() {
        let merkle_id: i64 = row.get(2);
        assert_eq!(
            merkle_id,
            (expected_start + idx + 1) as i64,
            "Merkle log IDs should be sequential"
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
    println!("   - Sequential merkle_log IDs verified");
    println!("   - Phase 1 validation: SUCCESS! ✨");
    
    Ok(())
}
