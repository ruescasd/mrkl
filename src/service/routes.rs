use axum::{
    extract::{Query, State},
    response::Json,
};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use serde::{Deserialize, Serialize};
use serde_json::json;

use super::AppState;
use crate::{ConsistencyProof, InclusionProof, LeafHash};

// Type alias for our JSON responses
type JsonResponse = Json<serde_json::Value>;

#[derive(Debug, Serialize, Deserialize)]
pub struct InclusionQuery {
    #[serde(with = "crate::service::routes::proof_bytes_format")]
    pub hash: Vec<u8>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ConsistencyQuery {
    /// The root hash of the historical tree to prove consistency with
    #[serde(with = "crate::service::routes::proof_bytes_format")]
    pub old_root: Vec<u8>,
}

// Fetches all entries from the database to rebuild the merkle tree
// Note: merkle_log may contain entries from multiple source tables,
// but the tree only cares about sequential ordering by id and the leaf hashes
pub async fn fetch_all_entries(
    db_pool: &deadpool_postgres::Pool,
) -> Result<(Vec<LeafHash>, Vec<(Vec<u8>, usize)>, i64), anyhow::Error> {
    println!("🔄 Fetching all entries from database...");

    // Get connection from pool
    let conn = db_pool.get().await?;

    // Query all rows from merkle_log which guarantees sequential order
    let rows = conn
        .query(
            "SELECT pl.id, pl.leaf_hash
             FROM merkle_log pl
             ORDER BY pl.id",
            &[],
        )
        .await?;

    // Create vectors for both the hashes and the index mappings
    let mut leaf_hashes = Vec::new();
    let mut index_mappings = Vec::new();
    let mut last_id = 0;

    // Collect all entries and mappings - now based only on leaf_hash
    for (idx, row) in rows.into_iter().enumerate() {
        let id = row.get::<_, i64>("id");
        let hash = row.get::<_, Vec<u8>>("leaf_hash");

        leaf_hashes.push(LeafHash { hash: hash.clone() });
        index_mappings.push((hash, idx));
        last_id = id;
    }

    println!("✅ Fetched {} entries from database", leaf_hashes.len());
    Ok((leaf_hashes, index_mappings, last_id))
}

// Handler for the /root endpoint
pub async fn get_merkle_root(State(state): State<AppState>) -> JsonResponse {
    let merkle_state = state.merkle_state.read();
    if merkle_state.tree.len() == 0 {
        return Json(json!({
            "status": "error",
            "error": "Merkle tree is empty"
        }));
    }
    let root = merkle_state.tree.root();
    Json(json!({
        "merkle_root": base64::engine::general_purpose::STANDARD.encode(&root),
        "tree_size": merkle_state.tree.len(),
        "last_processed_id": merkle_state.last_processed_id,
        "status": "ok"
    }))
}

// Handler for the /proof endpoint that generates inclusion proofs
pub async fn get_inclusion_proof(
    query_result: Result<Query<InclusionQuery>, axum::extract::rejection::QueryRejection>,
    State(state): State<AppState>,
) -> JsonResponse {
    // Handle query parameter parsing errors
    let Query(query) = match query_result {
        Ok(query) => query,
        Err(e) => {
            println!("🚫 Invalid proof request: {}", e);
            return Json(json!({
                "status": "error",
                "error": format!(
                    "Invalid request parameters: {}. The hash parameter must be base64 encoded.",
                    e
                )
            }));
        }
    };
    let merkle_state = state.merkle_state.read();

    // Look up the index in our O(1) index map
    let tree = &merkle_state.tree;

    // Try to generate and verify proof using our new tree's method
    let proof = match tree.prove_inclusion(&query.hash) {
        Ok(proof) => proof,
        Err(e) => {
            return Json(json!({
                "status": "error",
                "error": format!("Failed to generate inclusion proof: {:?}", e)
            }));
        }
    };

    // Verify the proof as a sanity check
    match tree.verify_inclusion(&query.hash, &proof) {
        Ok(true) => {
            // Get the index (safe because prove_inclusion succeeded)
            let index = tree.get_index(&query.hash).unwrap();

            // Create InclusionProof with all necessary data
            let inclusion_proof = InclusionProof {
                index,
                proof_bytes: proof.as_bytes().iter().copied().collect(),
                root: tree.root(),
                tree_size: tree.len(),
            };

            Json(json!({
                "status": "ok",
                "proof": inclusion_proof
            }))
        }
        Ok(false) | Err(_) => Json(json!({
            "status": "error",
            "error": "Generated proof failed verification"
        })),
    }
}

// Handler for the /rebuild endpoint
#[axum::debug_handler]
pub async fn trigger_rebuild(State(state): State<AppState>) -> JsonResponse {
    match rebuild_tree(&state).await {
        Ok((size, root, last_id)) => Json(json!({
            "status": "ok",
            "tree_size": size,
            "merkle_root": base64::engine::general_purpose::STANDARD.encode(root),
            "last_processed_id": last_id,
        })),
        Err(e) => Json(json!({
            "status": "error",
            "error": e.to_string()
        })),
    }
}

// Handler for the /consistency endpoint that generates consistency proofs
pub async fn get_consistency_proof(
    query_result: Result<Query<ConsistencyQuery>, axum::extract::rejection::QueryRejection>,
    State(state): State<AppState>,
) -> JsonResponse {
    // Handle query parameter parsing errors
    let Query(query) = match query_result {
        Ok(query) => query,
        Err(e) => {
            println!("🚫 Invalid consistency proof request: {}", e);
            return Json(json!({
                "status": "error",
                "error": format!(
                    "Invalid request parameters: {}. The old_root parameter must be base64 encoded.",
                    e
                )
            }));
        }
    };
    let merkle_state = state.merkle_state.read();
    let tree = &merkle_state.tree;

    // Try to generate consistency proof
    let proof = match tree.prove_consistency(&query.old_root) {
        Ok(proof) => proof,
        Err(e) => {
            return Json(json!({
                "status": "error",
                "error": format!("Failed to generate consistency proof: ProofError: {:?}", e)
            }));
        }
    };

    // Verify the proof
    match tree.verify_consistency(&query.old_root, &proof) {
        Ok(true) => {
            let old_size = tree.get_size_for_root(&query.old_root).unwrap(); // Safe because prove_consistency succeeded

            // Return the verified proof
            Json(json!({
                "status": "ok",
                "proof": ConsistencyProof {
                    old_tree_size: old_size,
                    old_root: query.old_root,
                    proof_bytes: proof.as_bytes().iter().copied().collect(),
                    new_root: tree.root(),
                    new_tree_size: tree.len(),
                }
            }))
        }
        Ok(false) | Err(_) => Json(json!({
            "status": "error",
            "error": "Generated proof failed verification"
        })),
    }
}

// Custom serialization for byte arrays to use base64
pub mod proof_bytes_format {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&BASE64.encode(bytes))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        String::deserialize(deserializer).and_then(|string| {
            BASE64
                .decode(string.as_bytes())
                .map_err(|err| Error::custom(err.to_string()))
        })
    }
}

/// Rebuilds the merkle tree and associated maps from scratch using database entries.
/// Returns the number of entries, final root hash, and last processed ID on success.
pub async fn rebuild_tree(state: &AppState) -> anyhow::Result<(usize, Vec<u8>, i64)> {
    println!("🔄 Starting tree rebuild...");

    // Measure database fetch time
    let fetch_start = std::time::Instant::now();
    let (leaf_hashes, _s, last_id) = fetch_all_entries(&state.db_pool).await?;
    let fetch_time = fetch_start.elapsed();
    println!(
        "📥 Database fetch completed in {}ms",
        fetch_time.as_millis()
    );

    // Measure computation time
    let compute_start = std::time::Instant::now();

    // Create new merkle state and build tree
    let mut merkle_state = crate::service::MerkleState::new();

    // Add leaf hashes to the tree (this will update internal maps)
    for leaf_hash in leaf_hashes {
        merkle_state.update_with_entry(leaf_hash, last_id);
    }

    // Get final state
    let size = merkle_state.tree.len();
    let root = merkle_state.tree.root();

    let compute_time = compute_start.elapsed();
    let total_time = fetch_time + compute_time;
    println!(
        "🧮 Tree computation completed in {}ms",
        compute_time.as_millis()
    );
    println!(
        "✅ Total rebuild time: {}ms for {} entries",
        total_time.as_millis(),
        size
    );

    // Update the merkle state
    *state.merkle_state.write() = merkle_state;

    Ok((size, root, last_id))
}
