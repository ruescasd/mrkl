
use anyhow::Result;
use tokio_postgres::{NoTls, AsyncMessage};
use futures_util::{StreamExt, TryStreamExt};
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
    let (client, mut connection) = tokio_postgres::connect(
        &std::env::var("DATABASE_URL").expect("DATABASE_URL must be set"),
        NoTls,
    ).await?;

    // Wrap the client in an Arc so it can be shared between tasks
    let client = Arc::new(client);
    
    // Initialize shared state with empty merkle tree and maps
    let app_state = AppState {
        merkle_tree: Arc::new(parking_lot::RwLock::new(MemoryBackedTree::new())),
        index_map: IndexMap::new(),
        root_map: RootMap::new(),
        client: client.clone(),
    };

    // Create channel for communication between tasks
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

    // Clone the client and state for the processing task
    let process_client = client.clone();
    let process_state = app_state.clone();

    // Spawn task to process received IDs
    tokio::spawn(async move {
        async move {
            while let Some(id) = rx.recv().await {
                println!("Received notification with ID: {}", id);
                // Fetch the actual row data
                if let Some(row) = process_client
                    .query_opt(
                        "SELECT id, data, leaf_hash FROM append_only_log WHERE id = $1",
                        &[&id],
                    )
                    .await?
                {
                    let data = row.get::<_, String>("data");
                    let hash = row.get::<_, Vec<u8>>("leaf_hash");
                    let leaf_hash = LeafHash::new(hash);
                    
                    // Update the merkle tree, index map, and root map
                    let mut tree = process_state.merkle_tree.write();
                    let idx = tree.len();
                    tree.push(leaf_hash);
                    process_state.index_map.insert(data.clone(), idx as usize);
                    let current_root = tree.root();
                    process_state.root_map.insert(current_root.as_bytes().to_vec(), tree.len() as usize);
                    
                    println!("📄 New row details:");
                    println!("   ID: {}", row.get::<_, i32>("id"));
                    println!("   Data: {}", data);
                    println!("   Current merkle root: {:?}", current_root.as_bytes());
                    println!("   Tree size: {}", tree.len());
                    println!("---");
                }
            }
            Ok::<_, tokio_postgres::Error>(())
        }
        .await
        .unwrap_or_else(|e| eprintln!("Error processing notification: {}", e));
    });

    // Create a stream of messages from the connection
    let mut messages = futures_util::stream::poll_fn(move |cx| connection.poll_message(cx))
        .map_err(|e| anyhow::anyhow!("Error polling messages: {}", e))
        .boxed();

    // Spawn the notification listener task
    let notification_handler = tokio::spawn(async move {
        // Process notifications as they arrive
        while let Some(message) = messages.try_next().await? {
            if let AsyncMessage::Notification(notification) = message {
                println!("🔔 Received notification on channel '{}'", notification.channel());
                println!("📝 Payload: {}", notification.payload());
                let row_id: i32 = notification.payload().parse()?;
                tx.send(row_id).unwrap();
            }
        }
        Ok::<_, anyhow::Error>(())
    });

    println!("Setting up notification listener...");

    // Start listening for notifications
    client.batch_execute("LISTEN new_row_channel").await?;
    println!("🎧 Listening for new rows on channel 'new_row_channel'...");

    // Do initial rebuild of the tree now that connection is established
    println!("Performing initial tree rebuild...");
    if let Ok((leaf_hashes, index_mappings)) = mrkl::fetch_all_entries(&app_state.client).await {
        // Clear all data structures
        app_state.index_map.clear();
        app_state.root_map.clear();

        // Rebuild the tree with the fetched entries
        let mut tree = app_state.merkle_tree.write();
        *tree = MemoryBackedTree::new();
        
        // Add leaf hashes to the tree
        for leaf_hash in leaf_hashes {
            tree.push(leaf_hash);
            // Record root hash after each insertion
            let current_root = tree.root();
            app_state.root_map.insert(current_root.as_bytes().to_vec(), tree.len() as usize);
        }
        
        // Update index mappings
        for (data, idx) in index_mappings {
            app_state.index_map.insert(data, idx);
        }
        
        println!("✅ Initial rebuild complete with {} entries", tree.len());
    } else {
        println!("⚠️ Failed to perform initial rebuild");
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

    // Wait for Ctrl+C, notification handler, or server to finish
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            println!("Received Ctrl+C, shutting down...");
        }
        result = notification_handler => {
            match result {
                Ok(Ok(_)) => println!("Notification handler completed successfully"),
                Ok(Err(e)) => println!("Notification handler error: {}", e),
                Err(e) => println!("Task join error: {}", e),
            }
        }
        _ = server => {
            println!("HTTP server shut down");
        }
    }

    Ok(())
}