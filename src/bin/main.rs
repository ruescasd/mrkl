
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
        next_id: Arc::new(parking_lot::RwLock::new(1)), // Will be updated by rebuild_tree
    };

    // Create channel for communication between tasks
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<String>();

    // Clone the state for the processing task
    let process_state = app_state.clone();

    // Spawn task to process notifications
    tokio::spawn(async move {
        // Buffer for out-of-order messages
        let mut pending_messages = std::collections::BTreeMap::new();

        async move {
            while let Some(payload) = rx.recv().await {
                // Parse the JSON payload
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(&payload) {
                    if let (Some(id), Some(data), Some(hash_b64)) = (
                        json.get("id").and_then(|v| v.as_i64()),
                        json.get("data").and_then(|v| v.as_str()),
                        json.get("hash").and_then(|v| v.as_str())
                    ) {
                        let id = id as i32;
                        let current_next_id = *process_state.next_id.read();
                        if id < current_next_id {
                            println!("⚠️ Ignoring already processed message id: {}", id);
                            continue;
                        }

                        if id > current_next_id {
                            // Store for later processing
                            println!("📥 Buffering out-of-order message id: {} (expecting {})", id, current_next_id);
                            pending_messages.insert(id, (data.to_string(), hash_b64.to_string()));
                            continue;
                        }

                        // Process the current message
                        process_message(&process_state, data, hash_b64);
                        *process_state.next_id.write() = id + 1;

                        // Process any buffered messages that are now in order
                        let mut next_expected_id = id + 1;
                        while let Some((data, hash_b64)) = pending_messages.remove(&next_expected_id) {
                            process_message(&process_state, &data, &hash_b64);
                            *process_state.next_id.write() = next_expected_id + 1;
                            next_expected_id += 1;
                        }
                    } else {
                        println!("⚠️ Missing id, data, or hash in notification payload");
                    }
                } else {
                    println!("⚠️ Failed to parse notification payload as JSON");
                }
            }
            Ok::<_, anyhow::Error>(())
        }
        .await
        .unwrap_or_else(|e| eprintln!("Error processing notification: {}", e));
    });

    // Helper function to process a message
    fn process_message(state: &AppState, data: &str, hash_b64: &str) {
        use base64::{Engine, engine::general_purpose::STANDARD};
        if let Ok(hash) = STANDARD.decode(hash_b64) {
            let leaf_hash = LeafHash::new(hash);
            
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
        } else {
            println!("⚠️ Failed to decode hash from base64");
        }
    }

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
                println!("📝 Raw payload received: '{}'", notification.payload());
                // Parse the payload to validate JSON structure
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(notification.payload()) {
                    println!("✅ Valid JSON received: {}", serde_json::to_string_pretty(&json).unwrap());
                } else {
                    println!("⚠️ Invalid JSON in notification payload");
                }
                tx.send(notification.payload().to_string()).unwrap();
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