use anyhow::Result;
use tokio_postgres::{NoTls, AsyncMessage};
use futures_util::{StreamExt, TryStreamExt};
use std::sync::Arc;
use axum::{
    routing::get,
    Router,
    extract::State,
    response::{Json, IntoResponse},
};
use std::net::SocketAddr;
use serde_json::json;
use ct_merkle::{
    mem_backed_tree::MemoryBackedTree,
    HashableLeaf, InclusionProof,
};
use sha2::Sha256;
use axum::extract::Query;
use serde::Deserialize;
use base64;

#[derive(Debug, Deserialize)]
struct ProofQuery {
    data: String,
}

// Type alias for our JSON responses
type JsonResponse = Json<serde_json::Value>;

// Fetches all entries from the database to rebuild the merkle tree
async fn fetch_all_entries(
    client: &tokio_postgres::Client,
) -> Result<Vec<String>, tokio_postgres::Error> {
    println!("🔄 Fetching all entries from database...");
    
    // Query all rows ordered by id to ensure consistent order
    let rows = client
        .query("SELECT id, data FROM append_only_log ORDER BY id", &[])
        .await?;
    
    // Collect all entries
    let entries: Vec<String> = rows
        .into_iter()
        .map(|row| row.get::<_, String>("data"))
        .collect();
    
    println!("✅ Fetched {} entries from database", entries.len());
    Ok(entries)
}

// Handler for the /root endpoint
async fn get_merkle_root(
    State(state): State<AppState>,
) -> JsonResponse {
    let tree = state.merkle_tree.read();
    let root = tree.root();
    Json(json!({
        "merkle_root": format!("{:?}", root.as_bytes()),
        "tree_size": tree.len(),
        "status": "ok"
    }))
}

// Handler for the /proof endpoint that generates inclusion proofs
async fn get_inclusion_proof(
    Query(query): Query<ProofQuery>,
    State(state): State<AppState>,
) -> JsonResponse {
    let tree = state.merkle_tree.read();
    
    // Get all items and find our target
    let tree_size = tree.len();
    let mut found_idx = None;
    
    // Linear search through the tree items
    for i in 0..tree_size {
        if tree.get(i as usize) == Some(&query.data) {
            found_idx = Some(i);
            break;
        }
    }
    
    match found_idx {
        Some(idx) => {
            // Generate the proof
            let proof = tree.prove_inclusion(idx as usize);
            let root = tree.root();
            
            // Verify the proof immediately as a sanity check
            match root.verify_inclusion(&query.data, idx as u64, &proof) {
                Ok(()) => {
                    // Convert proof to bytes for transmission
                    let proof_bytes: Vec<u8> = proof
                        .as_bytes()
                        .iter()
                        .copied()
                        .collect();
                    
                    Json(json!({
                        "status": "ok",
                        "index": idx,
                        "proof": base64::encode(proof_bytes),
                        "root": format!("{:?}", root.as_bytes())
                    }))
                },
                Err(e) => Json(json!({
                    "status": "error",
                    "error": format!("Generated proof failed verification: {}", e)
                }))
            }
        },
        None => Json(json!({
            "status": "error",
            "error": "Data not found in the tree"
        }))
    }
}


// Handler for the /rebuild endpoint
#[axum::debug_handler]
async fn trigger_rebuild(
    State(state): State<AppState>,
) -> JsonResponse {
    // First fetch all entries without holding the lock
    match fetch_all_entries(&state.client).await {
        Ok(entries) => {
            // Now rebuild the tree with the fetched entries
            let mut tree = state.merkle_tree.write();
            
            // Clear and rebuild
            *tree = MemoryBackedTree::new();
            for entry in entries {
                tree.push(entry);
            }
            
            // Get final state
            let size = tree.len();
            let root = tree.root();
            let root_bytes = root.as_bytes().to_vec();
            
            // Drop the write guard before creating response
            drop(tree);
            
            Json(json!({
                "status": "ok",
                "tree_size": size,
                "merkle_root": format!("{:?}", root_bytes)
            }))
        },
        Err(e) => Json(json!({
            "status": "error",
            "error": e.to_string()
        }))
    }
}

// Shared state between HTTP server and notification processor
#[derive(Clone)]
struct AppState {
    // The memory-backed merkle tree storing our log entries
    merkle_tree: Arc<parking_lot::RwLock<MemoryBackedTree<Sha256, String>>>,
    // Database client for operations that need it
    client: Arc<tokio_postgres::Client>,
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

    // Do initial rebuild of the tree now that connection is established
    println!("Performing initial tree rebuild...");
    if let Ok(entries) = fetch_all_entries(&app_state.client).await {
        let mut tree = app_state.merkle_tree.write();
        *tree = MemoryBackedTree::new();
        for entry in entries {
            tree.push(entry);
        }
        println!("✅ Initial rebuild complete with {} entries", tree.len());
    } else {
        println!("⚠️ Failed to perform initial rebuild");
    }

    // Build our application with routes
    let app = Router::new()
        .route("/root", get(get_merkle_root))
        .route("/rebuild", get(trigger_rebuild))
        .route("/proof", get(get_inclusion_proof))
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