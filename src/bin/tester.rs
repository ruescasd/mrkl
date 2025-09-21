use anyhow::Result;
use mrkl::test_client::TestClient;
use rand::prelude::*;
use std::time::Duration;
use tokio::time;

/// Regular interval test mode: adds one entry every N seconds
async fn run_interval_test(client: &TestClient, interval_secs: u64) -> Result<()> {
    let mut counter = 0;
    let mut rng = rand::thread_rng();

    println!("Starting regular interval test (every {} seconds)...", interval_secs);
    loop {
        counter += 1;
        
        // Generate some random data
        let random_suffix: u32 = rng.r#gen();
        let data = format!("{}-{}", counter, random_suffix);
        
        // Add the entry
        println!("➕ Adding entry: {}", data);
        let id = client.add_entry(&data).await?;
        println!("✅ Added entry with ID: {}", id);
        
        // Small delay to allow the listener to process
        time::sleep(Duration::from_millis(100)).await;
        
        // Get and verify the proof
        println!("🔍 Getting proof for entry...");
        let proof = client.get_proof(&data).await?;
        println!("✅ Got proof at index {}", proof.index);
        
        // Get the current root
        let root = client.get_root().await?;
        println!("🌳 Current merkle root: {:?}", root);
        
        // Verify both the root matches and the proof is valid
        assert_eq!(root, proof.root, "Merkle root mismatch!");
        assert!(proof.verify(&data)?, "Proof verification failed!");
        println!("✅ Verified proof is valid and merkle root matches\n");
        
        // Wait for the next interval
        time::sleep(Duration::from_secs(interval_secs)).await;
    }
}

/// Burst test mode: adds multiple entries in quick succession
async fn run_burst_test(client: &TestClient, burst_size: usize, interval_secs: u64) -> Result<()> {
    let mut counter = 0;
    let mut rng = rand::thread_rng();

    println!("Starting burst test (burst size: {}, every {} seconds)...", burst_size, interval_secs);
    loop {
        println!("\n🌊 Starting burst of {} entries...", burst_size);
        
        // Vector to store all data for verification
        let mut entries = Vec::new();
        
        // Add burst of entries
        for _ in 0..burst_size {
            counter += 1;
            let random_suffix: u32 = rng.r#gen();
            let data = format!("Burst entry {} (random: {})", counter, random_suffix);
            
            println!("➕ Adding entry: {}", data);
            let id = client.add_entry(&data).await?;
            println!("✅ Added entry with ID: {}", id);
            
            entries.push(data);
        }
        
        // Small delay to allow the listener to process
        time::sleep(Duration::from_millis(500)).await;
        
        // Get the final root after the burst
        let root = client.get_root().await?;
        println!("🌳 Final merkle root after burst: {:?}", root);
        
        // Verify proofs for all entries
        println!("🔍 Verifying proofs for all entries in burst...");
        for data in entries {
            let proof = client.get_proof(&data).await?;
            assert_eq!(root, proof.root, "Merkle root mismatch for entry: {}", data);
            assert!(proof.verify(&data)?, "Proof verification failed for entry: {}", data);
            println!("✅ Verified proof for entry at index {}", proof.index);
        }
        
        // Wait for the next burst
        time::sleep(Duration::from_secs(interval_secs)).await;
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load environment variables from .env file
    dotenv::dotenv().ok();
    
    let client = TestClient::new("http://127.0.0.1:3000").await?;
    
    // Parse command line arguments for test mode
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("burst") => {
            let burst_size = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);
            let interval = args.get(3).and_then(|s| s.parse().ok()).unwrap_or(10);
            run_burst_test(&client, burst_size, interval).await?;
        }
        _ => {
            let interval = args.get(2).and_then(|s| s.parse().ok()).unwrap_or(5);
            run_interval_test(&client, interval).await?;
        }
    }
    
    Ok(())
}