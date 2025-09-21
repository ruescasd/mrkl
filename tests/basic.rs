use anyhow::Result;
use mrkl::client::Client;
use tokio::time::{sleep, Duration};
use serial_test::serial;

async fn setup_client() -> Result<Client> {
    // Load environment variables from .env file
    dotenv::dotenv().ok();
    Client::new("http://localhost:3000").await
}

#[serial]
#[tokio::test]
async fn test_inclusion_proofs() -> Result<()> {
    let client = setup_client().await?;
    
    // Add several entries
    let entries = vec!["data1", "data2", "data3", "data4", "data5"];
    for entry in &entries {
        client.add_entry(entry).await?;
        sleep(Duration::from_millis(100)).await;
    }
    
    // Get the current root
    let root = client.get_root().await?;
    
    // Verify inclusion proofs for all entries
    for entry in &entries {
        let proof = client.get_proof(entry).await?;
        
        // Verify proof has correct root
        assert_eq!(proof.root, root, "Proof root should match current tree root for entry '{}'", entry);
        
        // Verify the proof itself
        assert!(proof.verify(entry)?, "Proof verification should succeed for entry '{}'", entry);
        
        println!("✅ Verified inclusion proof for '{}' at index {}", entry, proof.index);
    }
    
    Ok(())
}

#[serial]
#[tokio::test]
async fn test_consistency_proofs() -> Result<()> {
    let client = setup_client().await?;
    let mut historical_roots = Vec::new();
    
    // Add entries in batches of 3 and store roots
    for i in 0..3 {
        for j in 0..3 {
            let entry = format!("batch{}-entry{}", i + 1, j + 1);
            client.add_entry(&entry).await?;
            sleep(Duration::from_millis(100)).await;
        }
        
        // Store root after each batch
        let root = client.get_root().await?;
        println!("📸 Storing root after batch {}", i + 1);
        historical_roots.push(root);
    }

    // Add a final entry to change the tree state
    let entry = format!("last");
    client.add_entry(&entry).await?;
    sleep(Duration::from_millis(100)).await;
    
    // Get final tree state
    let final_root = client.get_root().await?;
    
    // Verify consistency between each historical root and final state
    for (i, historical_root) in historical_roots.iter().enumerate() {
        println!("🔍 Verifying consistency with batch {}", i + 1);
        assert!(
            client.verify_tree_consistency(historical_root.clone()).await?,
            "Tree should be consistent with historical root from batch {}", i + 1
        );
    }
    
    println!("🔍 Consistency of root with itself should fail");
    assert!(
        client.verify_tree_consistency(final_root).await.is_err(),
        "Consistency of root with itself should fail"
    );
    
    Ok(())
}

#[serial]
#[tokio::test]
async fn test_burst_operations() -> Result<()> {
    let client = setup_client().await?;
    
    // Create a batch of entries to add in quick succession
    let entries: Vec<String> = (1..=10)
        .map(|i| format!("burst-entry-{}", i))
        .collect();
    
    // Add all entries as quickly as possible
    println!("🌊 Starting burst of {} entries...", entries.len());
    for entry in &entries {
        client.add_entry(entry).await?;
    }
    
    // Give the system a moment to process all entries
    sleep(Duration::from_millis(500)).await;
    
    // Get final root
    let root = client.get_root().await?;
    println!("🌳 Final merkle root after burst: {:?}", root);
    
    // Verify all entries are properly included
    for entry in &entries {
        let proof = client.get_proof(entry).await?;
        assert_eq!(root, proof.root, "Merkle root mismatch for entry: {}", entry);
        assert!(proof.verify(entry)?, "Proof verification failed for entry: {}", entry);
        println!("✅ Verified proof for '{}' at index {}", entry, proof.index);
    }
    
    Ok(())
}