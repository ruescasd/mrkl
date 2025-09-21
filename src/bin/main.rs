
use anyhow::Result;
use tokio_postgres::NoTls;
use std::sync::Arc;
use axum::{
    routing::get,
    Router,
};
use std::net::SocketAddr;
use ct_merkle::{
    mem_backed_tree::MemoryBackedTree,
};
use mrkl::{LeafHash, IndexMap, RootMap};
use mrkl::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();

    println!("Connecting to database...");
    
    // Connect to the database and split into client and raw connection
    let (client, connection) = tokio_postgres::connect(
        &std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
        NoTls,
    ).await?;

    // Wrap the client in an Arc so it can be shared between tasks
    let client = Arc::new(client);
    
    // Spawn a task to drive the connection
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Initialize shared state with empty merkle tree and maps
    let app_state = AppState {
        merkle_tree: Arc::new(parking_lot::RwLock::new(MemoryBackedTree::new())),
        index_map: IndexMap::new(),
        root_map: RootMap::new(),
        client: client.clone(),
    };

    // Clone the state for the processing task
    let process_state = app_state.clone();

    // Spawn the batch processing task
    let processor = tokio::spawn(async move {
        let batch_size = 10000;
        let interval = std::time::Duration::from_secs(1);

        loop {
            // Time the batch processing (movement from append_only_log to processed_log)
            let batch_start = std::time::Instant::now();
            
            let batch_result = process_state.client.query_one(
                "SELECT * FROM process_next_batch($1)", 
                &[&batch_size]
            ).await;
            
            let batch_duration = batch_start.elapsed();
            
            match batch_result {
                Ok(row) => {
                    let rows_processed: i64 = row.get("rows_processed");
                    if rows_processed > 0 {
                        let first_id: i64 = row.get("first_id");
                        let last_id: i64 = row.get("last_id");
                        
                        // Time the retrieval from processed_log
                        let fetch_start = std::time::Instant::now();
                        
                        let fetch_result = process_state.client.query(
                            "SELECT data, leaf_hash 
                             FROM processed_log 
                             WHERE id BETWEEN $1 AND $2 
                             ORDER BY id",
                            &[&first_id, &last_id]
                        ).await;
                        
                        let fetch_duration = fetch_start.elapsed();
                        
                        match fetch_result {
                            Ok(rows) => {
                                // Start timing the tree construction
                                let tree_start = std::time::Instant::now();
                                
                                // Update the merkle tree and maps
                                let mut tree = process_state.merkle_tree.write();
                                for row in rows {
                                    let data: String = row.get("data");
                                    let hash: Vec<u8> = row.get("leaf_hash");
                                    let leaf_hash = LeafHash::new(hash);
                                    let idx = tree.len();
                                    tree.push(leaf_hash);
                                    process_state.index_map.insert(data, idx as usize);
                                    let current_root = tree.root();
                                    process_state.root_map.insert(current_root.as_bytes().to_vec(), tree.len() as usize);
                                }
                                
                                let tree_duration = tree_start.elapsed();
                                
                                // Log timing information
                                println!("Batch stats:");
                                println!("  Rows processed: {}", rows_processed);
                                println!("  Batch processing time: {:?}", batch_duration);
                                println!("  Fetch from processed_log time: {:?}", fetch_duration);
                                println!("  Tree construction time: {:?}", tree_duration);
                            }
                            Err(e) => println!("⚠️ Error fetching processed rows: {}", e),
                        }
                    }
                }
                Err(e) => {
                    if !e.to_string().contains("Another processing batch is running") {
                        println!("⚠️ Error processing batch: {}", e);
                    }
                }
            }
            
            // Wait for next interval
            tokio::time::sleep(interval).await;
        }
    });

    // Do initial rebuild of the tree now that connection is established
    println!("Performing initial tree rebuild...");
    match mrkl::rebuild_tree(&app_state).await {
        Ok((size, _)) => println!("✅ Initial rebuild complete with {} entries", size),
        Err(e) => println!("⚠️ Failed to perform initial rebuild: {}", e)
    }

    // Build our application with routes
    let app = Router::new()
        .route("/root", get(mrkl::get_merkle_root))
        .route("/rebuild", get(mrkl::trigger_rebuild))
        .route("/proof", get(mrkl::get_inclusion_proof))
        .route("/consistency", get(mrkl::get_consistency_proof))
        .with_state(app_state);

    // Run our HTTP server
    let addr = SocketAddr::from(([127, 0, 0, 1], 3000));
    println!("🌐 HTTP server listening on http://{}", addr);
    let server = axum::serve(
        tokio::net::TcpListener::bind(addr).await?,
        app
    );

    // Wait for Ctrl+C, processor, or server to finish
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down...");
        }
        result = processor => {
            match result {
                Ok(_) => println!("Processor completed successfully"),
                Err(e) => println!("Processor error: {}", e),
            }
        }
        _ = server => {
            println!("HTTP server shut down");
        }
    }

    Ok(())
}