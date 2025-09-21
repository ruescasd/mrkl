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
    HashableLeaf, InclusionProof, RootHash
};
use sha2::{Sha256, digest::Output};
use axum::extract::Query;
use serde::Deserialize;
use mrkl::{LeafHash, IndexMap, RootMap, MerkleProof, ConsistencyProof};


#[derive(Debug, Deserialize)]
struct ProofQuery {
    data: String,
}

#[derive(Debug, Deserialize)]
struct ConsistencyQuery {
    /// The root hash of the historical tree to prove consistency with
    #[serde(with = "mrkl::proof_bytes_format")]
    old_root: Vec<u8>,
}

// Type alias for our JSON responses
type JsonResponse = Json<serde_json::Value>;

// Fetches all entries from the database to rebuild the merkle tree
async fn fetch_all_entries(
    client: &tokio_postgres::Client,
) -> Result<(Vec<LeafHash>, Vec<(String, usize)>), tokio_postgres::Error> {
    println!("🔄 Fetching all entries from database...");
    
    // Query all rows ordered by id to ensure consistent order
    let rows = client
        .query("SELECT id, data, leaf_hash FROM append_only_log ORDER BY id", &[])
        .await?;
    
    // Create vectors for both the hashes and the index mappings
    let mut leaf_hashes = Vec::new();
    let mut index_mappings = Vec::new();
    
    // Collect all entries and mappings
    for (idx, row) in rows.into_iter().enumerate() {
        let data = row.get::<_, String>("data");
        let hash = row.get::<_, Vec<u8>>("leaf_hash");
        
        leaf_hashes.push(LeafHash { hash });
        index_mappings.push((data, idx));
    }
    
    println!("✅ Fetched {} entries from database", leaf_hashes.len());
    Ok((leaf_hashes, index_mappings))
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
    
    // Look up the index in our O(1) index map
    let found_idx = state.index_map.get(&query.data);
    
    // First compute the hash of our query data
    let query_hash = LeafHash::from_data(&query.data);
    
    match found_idx {
        Some(idx) => {
            // Generate the proof
            let proof = tree.prove_inclusion(idx);
            let root = tree.root();
            
            // Verify the proof immediately as a sanity check
            match root.verify_inclusion(&query_hash, idx as u64, &proof) {
                Ok(()) => {
                    // Create a MerkleProof with all necessary data
                    let merkle_proof = MerkleProof {
                        index: idx,
                        proof_bytes: proof.as_bytes().iter().copied().collect(),
                        root: root.as_bytes().to_vec(),
                        tree_size: tree.len() as usize,
                    };
                    
                    Json(json!({
                        "status": "ok",
                        "proof": merkle_proof
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
        Ok((leaf_hashes, index_mappings)) => {
            // Clear all data structures
            state.index_map.clear();
            state.root_map.clear();

            // Now rebuild the tree with the fetched entries
            let mut tree = state.merkle_tree.write();
            
            // Clear and rebuild tree, index map, and root map
            *tree = MemoryBackedTree::new();
            for (_idx, leaf_hash) in leaf_hashes.into_iter().enumerate() {
                tree.push(leaf_hash);
                // Record root hash after each insertion
                let current_root = tree.root();
                state.root_map.insert(current_root.as_bytes().to_vec(), tree.len() as usize);
            }
            
            // Update index mappings
            for (data, idx) in index_mappings {
                state.index_map.insert(data, idx);
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
    // The memory-backed merkle tree storing just the leaf hashes
    merkle_tree: Arc<parking_lot::RwLock<MemoryBackedTree<Sha256, LeafHash>>>,
    // Mapping from data strings to their indices in the tree
    index_map: IndexMap,
    // Mapping from root hashes to their tree sizes
    root_map: RootMap,
    // Database client for operations that need it
    client: Arc<tokio_postgres::Client>,
}

// Handler for the /consistency endpoint that generates consistency proofs
async fn get_consistency_proof(
    Query(query): Query<ConsistencyQuery>,
    State(state): State<AppState>,
) -> JsonResponse {

    // Look up size for historical root
    let old_size = match state.root_map.get_size(&query.old_root) {
        Some(size) => size,
        None => return Json(json!({
            "status": "error",
            "error": "Historical root hash not found in tree history"
        }))
    };

    // Get current tree info
    let tree = state.merkle_tree.read();
    let current_size = tree.len() as usize;
    let current_root = tree.root();

    // Validate the sizes
    if old_size > current_size {
        return Json(json!({
            "status": "error",
            "error": format!("Invalid request: Historical tree size {} is larger than current size {}", 
                           old_size, current_size)
        }));
    }

    if old_size == 0 {
        return Json(json!({
            "status": "error",
            "error": "Invalid request: Cannot generate consistency proof for empty tree"
        }));
    }

    if old_size == current_size {
        return Json(json!({
            "status": "error", 
            "error": "Invalid request: No new entries since historical root"
        }));
    }
    
    // Calculate how many items were added since historical root
    let num_additions = current_size - old_size;

    // Create RootHash objects for verification
    let mut old_digest = Output::<Sha256>::default();
    old_digest.copy_from_slice(&query.old_root);
    let old_root = RootHash::<Sha256>::new(old_digest, old_size as u64);
    
    // Generate consistency proof using current tree state
    let proof = tree.prove_consistency(num_additions);
    
    if let Err(e) = current_root.verify_consistency(&old_root, &proof) {
        println!("Consistency proof verification failed: {}", e);
        return Json(json!({
            "status": "error",
            "error": format!("Generated proof failed verification: {}", e)
        }));
    }

    // Return the verified proof
    Json(json!({
        "status": "ok",
        "proof": ConsistencyProof {
            old_tree_size: old_size,
            old_root: query.old_root,
            proof_bytes: proof.as_bytes().iter().copied().collect(),
            new_root: current_root.as_bytes().to_vec(),
            new_tree_size: current_size,
        }
    }))
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
    if let Ok((leaf_hashes, index_mappings)) = fetch_all_entries(&app_state.client).await {
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
        .route("/root", get(get_merkle_root))
        .route("/rebuild", get(trigger_rebuild))
        .route("/proof", get(get_inclusion_proof))
        .route("/consistency", get(get_consistency_proof))
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