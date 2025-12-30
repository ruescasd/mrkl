use crate::{ConsistencyProof, InclusionProof};
use anyhow::Result;
use base64::Engine;
use reqwest::Client as HttpClient;
use sha2::{Digest, Sha256};
use std::time::Duration;

/// Production HTTP client for interacting with the merkle tree service
pub struct Client {
    /// HTTP client for making requests to the API
    http_client: HttpClient,
    /// Base URL of the merkle tree service API
    api_base_url: String,
}

impl Client {
    /// Creates a new HTTP client for the merkle tree service
    ///
    /// # Errors
    ///
    /// Returns an error if the `HTTPClient` build fails.
    pub fn new(api_base_url: &str) -> Result<Self> {
        // Set up HTTP client with reasonable timeouts
        let http_client = HttpClient::builder()
            .timeout(Duration::from_secs(10))
            .build()?;

        Ok(Self {
            http_client,
            api_base_url: api_base_url.to_string(),
        })
    }

    /// Gets the current size (number of entries) of the log
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails, if the response is invalid or malformed,
    /// or if the server returns an error status.
    pub async fn get_log_size(&self, log_name: &str) -> Result<u64> {
        let response = self
            .http_client
            .get(format!("{}/logs/{}/size", self.api_base_url, log_name))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        // Check for error response
        if response.get("status").and_then(|v| v.as_str()) == Some("error") {
            let error_msg = response
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(anyhow::anyhow!("Error getting log size: {error_msg}"));
        }

        let size = response
            .get("size")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| anyhow::anyhow!("Invalid size response"))?;

        Ok(size)
    }

    /// Gets the current merkle root
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails, if the response is invalid or malformed,
    /// if the base64 decoding of the root bytes fails, or if the server returns an error status.
    pub async fn get_root(&self, log_name: &str) -> Result<Vec<u8>> {
        let response = self
            .http_client
            .get(format!("{}/logs/{}/root", self.api_base_url, log_name))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        // Check for error response
        if response.get("status").and_then(|v| v.as_str()) == Some("error") {
            let error_msg = response
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(anyhow::anyhow!("Error getting root: {error_msg}"));
        }

        // Parse the base64 encoded root from the response
        let root_b64 = response
            .get("merkle_root")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Invalid root response"))?;

        // Decode base64 back to bytes
        let root_bytes = base64::engine::general_purpose::STANDARD.decode(root_b64)?;

        Ok(root_bytes)
    }

    /// Gets an inclusion proof for a given hash or piece of data
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails, if the response is invalid or malformed,
    /// or if the server returns an error status.
    pub async fn get_inclusion_proof(&self, log_name: &str, data: &Vec<u8>) -> Result<InclusionProof> {
        // Compute the hash from the data
        // let mut hasher = Sha256::new();
        // hasher.update(data.as_bytes());
        // let hash_result = hasher.finalize();

        let query = crate::service::InclusionQuery {
            hash: data.clone(),
        };

        // Make the request with the base64 encoded hash
        let response = self
            .http_client
            .get(format!("{}/logs/{}/proof", self.api_base_url, log_name))
            .query(&query)
            .send()
            .await?;

        // Parse the response JSON into our MerkleProof struct
        let response = response.json::<serde_json::Value>().await?;

        // Check for error response
        if response.get("status").and_then(|v| v.as_str()) == Some("error") {
            let error_msg = response
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(anyhow::anyhow!("Error getting inclusion proof: {error_msg}"));
        }

        // Parse into our structured MerkleProof type
        let proof = response
            .get("proof")
            .ok_or_else(|| anyhow::anyhow!("Missing proof field in response"))?;
        let proof = serde_json::from_value(proof.clone())?;
        Ok(proof)
    }

    /// Gets a consistency proof proving that the current tree state is consistent with an older root hash
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails, if the response is invalid or malformed,
    /// or if the server returns an error status.
    pub async fn get_consistency_proof(
        &self,
        log_name: &str,
        old_root: Vec<u8>,
    ) -> Result<ConsistencyProof> {
        let query = crate::service::ConsistencyQuery { old_root };

        let response = self
            .http_client
            .get(format!(
                "{}/logs/{}/consistency",
                self.api_base_url, log_name
            ))
            .query(&query)
            .send()
            .await?;

        // Parse the response JSON into our ConsistencyProof struct
        let response = response.json::<serde_json::Value>().await?;

        // Check for error response
        if response.get("status").and_then(|v| v.as_str()) == Some("error") {
            let error_msg = response
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(anyhow::anyhow!("Error getting consistency proof: {error_msg}"));
        }

        // Parse into our structured ConsistencyProof type
        let proof = response
            .get("proof")
            .ok_or_else(|| anyhow::anyhow!("Missing proof field in response"))?;
        let proof = serde_json::from_value(proof.clone())?;
        Ok(proof)
    }

    /// Verifies that the current tree state is consistent with a previously observed state.
    /// Returns true if the current tree is a descendant of `old_root` (i.e., old tree is a prefix).
    ///
    /// # Errors
    ///
    /// Returns an error if fetching the consistency proof or current root fails,
    /// or if proof verification fails.
    pub async fn verify_tree_consistency(&self, log_name: &str, old_root: Vec<u8>) -> Result<Vec<u8>> {
        // Get consistency proof between the old root and current state
        let proof = self.get_consistency_proof(log_name, old_root.clone()).await?;

        // Verify the proof cryptographically
        proof
            .verify(&old_root)
            .map_err(|e| anyhow::anyhow!("Proof verification failed: {e}"))?;
        
        Ok(proof.new_root)
    }

    /// Checks if a leaf (identified by its hash) exists in the log
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails, if the response is invalid or malformed,
    /// or if the server returns an error status.
    pub async fn has_leaf(&self, log_name: &str, data: &str) -> Result<bool> {
        // Compute the hash from the data
        let mut hasher = Sha256::new();
        hasher.update(data.as_bytes());
        let hash_result = hasher.finalize();

        let query = crate::service::HasLeafQuery {
            hash: hash_result.to_vec(),
        };

        // Make the request
        let response = self
            .http_client
            .get(format!("{}/logs/{}/has_leaf", self.api_base_url, log_name))
            .query(&query)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        // Check for error response
        if response.get("status").and_then(|v| v.as_str()) == Some("error") {
            let error_msg = response
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(anyhow::anyhow!("Error checking leaf existence: {error_msg}"));
        }

        // Extract exists field
        let exists = response
            .get("exists")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| anyhow::anyhow!("Invalid has_leaf response"))?;

        Ok(exists)
    }

    /// Checks if a historical root exists in the log
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails, if the response is invalid or malformed,
    /// or if the server returns an error status.
    pub async fn has_root(&self, log_name: &str, root: Vec<u8>) -> Result<bool> {
        let query = crate::service::HasRootQuery { root };

        // Make the request
        let response = self
            .http_client
            .get(format!("{}/logs/{}/has_root", self.api_base_url, log_name))
            .query(&query)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        // Check for error response
        if response.get("status").and_then(|v| v.as_str()) == Some("error") {
            let error_msg = response
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(anyhow::anyhow!("Error checking root existence: {error_msg}"));
        }

        // Extract exists field
        let exists = response
            .get("exists")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| anyhow::anyhow!("Invalid has_root response"))?;

        Ok(exists)
    }

    /// Checks if a log exists on the server
    ///
    /// # Errors
    ///
    /// Returns an error if the HTTP request fails, if the response is invalid or malformed,
    /// or if the server returns an error status.
    pub async fn has_log(&self, log_name: &str) -> Result<bool> {
        let response = self
            .http_client
            .get(format!("{}/logs/{}/exists", self.api_base_url, log_name))
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        // Check for error response
        if response.get("status").and_then(|v| v.as_str()) == Some("error") {
            let error_msg = response
                .get("error")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown error");
            return Err(anyhow::anyhow!("Error checking log existence: {error_msg}"));
        }

        // Extract exists field
        let exists = response
            .get("exists")
            .and_then(serde_json::Value::as_bool)
            .ok_or_else(|| anyhow::anyhow!("Invalid has_log response"))?;

        Ok(exists)
    }
}
