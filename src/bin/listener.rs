use anyhow::Result;
use tokio_postgres::{NoTls, AsyncMessage};
use futures_util::{StreamExt, TryStreamExt};
use std::sync::Arc;
use axum::{
    routing::get,
    Router,
    extract::State,
    response::Json,
};
use std::net::SocketAddr;
use serde_json::json;
use ct_merkle::{
    mem_backed_tree::MemoryBackedTree,
    HashableLeaf,
};
use sha2::Sha256;

// Handler for the /root endpoint
async fn get_merkle_root(
    State(state): State<AppState>,
) -> Json<serde_json::Value> {
    let tree = state.merkle_tree.read();
    let root = tree.root();
    Json(json!({
        "merkle_root": format!("{:?}", root.as_bytes()),
        "tree_size": tree.len(),
        "status": "ok"
    }))
}

// Shared state between HTTP server and notification processor
#[derive(Clone)]
struct AppState {
    // The memory-backed merkle tree storing our log entries
    merkle_tree: Arc<parking_lot::RwLock<MemoryBackedTree<Sha256, String>>>,
}

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
    
    // Initialize shared state with empty merkle tree
    let app_state = AppState {
        merkle_tree: Arc::new(parking_lot::RwLock::new(MemoryBackedTree::new())),
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
                        "SELECT id, data FROM append_only_log WHERE id = $1",
                        &[&id],
                    )
                    .await?
                {
                    let data = row.get::<_, String>("data");
                    
                    // Update the merkle tree with the new entry
                    let mut tree = process_state.merkle_tree.write();
                    tree.push(data.clone());
                    let current_root = tree.root();
                    
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


    // Build our application with a route
    let app = Router::new()
        .route("/root", get(get_merkle_root))
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