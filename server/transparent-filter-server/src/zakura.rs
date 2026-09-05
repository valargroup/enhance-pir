//! Zakura JSON-RPC client.
//!
//! Deliberately a separate client from the one in `enhance-pir-server`. This
//! service must be able to run without the PIR coordinator, and depending on
//! that crate would drag in the whole iPIR stack for the sake of sixty lines of
//! JSON-RPC. The cookie handling, auth and error shapes match it on purpose.
//!
//! Blocks and transactions are fetched raw (verbosity 0) and parsed locally.
//! The verbose JSON form renders scripts through the node's own notion of what
//! is standard; raw parsing yields every script exactly as it appears on chain,
//! which is what a filter must cover.

use serde::de::DeserializeOwned;
use serde_json::json;
use std::path::Path;
use zakura_chain::block::Block;
use zakura_chain::serialization::ZcashDeserialize;
use zakura_chain::transaction::Transaction;

#[derive(Debug, thiserror::Error)]
pub enum ZakuraError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("cookie error: {0}")]
    Cookie(#[from] std::io::Error),
    #[error("invalid RPC cookie")]
    InvalidCookie,
    #[error("RPC returned {0}: {1}")]
    Rpc(i64, String),
    #[error("RPC response is missing a result")]
    MissingResult,
    #[error("invalid hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("could not parse block: {0}")]
    Block(String),
    #[error("could not parse transaction {0}: {1}")]
    Transaction(String, String),
    #[error("malformed RPC batch response: {0}")]
    Batch(String),
}

#[derive(Clone)]
pub struct ZakuraClient {
    http: reqwest::Client,
    rpc_url: String,
    username: String,
    password: String,
}

#[derive(serde::Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

/// One entry of a JSON-RPC batch response.
#[derive(serde::Deserialize)]
struct BatchEntry {
    id: Option<i64>,
    result: Option<String>,
    error: Option<RpcError>,
}

#[derive(serde::Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

impl ZakuraClient {
    pub fn from_cookie_file(
        rpc_url: impl Into<String>,
        cookie_path: impl AsRef<Path>,
    ) -> Result<Self, ZakuraError> {
        let cookie = std::fs::read_to_string(cookie_path)?;
        let (username, password) = cookie
            .trim()
            .split_once(':')
            .ok_or(ZakuraError::InvalidCookie)?;
        if username.is_empty() || password.is_empty() {
            return Err(ZakuraError::InvalidCookie);
        }
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(120))
                .build()?,
            rpc_url: rpc_url.into(),
            username: username.to_string(),
            password: password.to_string(),
        })
    }

    pub async fn tip_height(&self) -> Result<u64, ZakuraError> {
        self.call("getblockcount", json!([])).await
    }

    /// Block hash at `height`, in RPC display order.
    pub async fn block_hash(&self, height: u64) -> Result<String, ZakuraError> {
        self.call("getblockhash", json!([height])).await
    }

    /// The genesis block hash, this chain's identity.
    pub async fn genesis_hash(&self) -> Result<String, ZakuraError> {
        self.block_hash(0).await
    }

    /// The full block at `height`, parsed from its canonical serialization.
    pub async fn block(&self, height: u64) -> Result<(String, Block), ZakuraError> {
        let raw_hex: String = self
            .call("getblock", json!([height.to_string(), 0]))
            .await?;
        let raw = hex::decode(raw_hex)?;
        let block = Block::zcash_deserialize(raw.as_slice())
            .map_err(|error| ZakuraError::Block(error.to_string()))?;
        let hash = block.hash().to_string();
        Ok((hash, block))
    }

    /// A transaction by id, parsed from its canonical serialization.
    ///
    /// `txid` is display hex, as the RPC expects. Returns `Ok(None)` when the
    /// node does not have the transaction, which the caller must treat as a
    /// construction failure rather than as an absent script.
    pub async fn transaction(&self, txid: &str) -> Result<Option<Transaction>, ZakuraError> {
        let raw_hex: String = match self.call("getrawtransaction", json!([txid, 0])).await {
            Ok(hex) => hex,
            // -5 is "No information available about transaction" in the
            // zcashd-compatible error set.
            Err(ZakuraError::Rpc(-5, _)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let raw = hex::decode(raw_hex)?;
        let transaction = Transaction::zcash_deserialize(raw.as_slice())
            .map_err(|error| ZakuraError::Transaction(txid.to_string(), error.to_string()))?;
        Ok(Some(transaction))
    }

    /// Fetches several transactions in one JSON-RPC batch.
    ///
    /// Returns results positionally, `None` where the node does not have the
    /// transaction. Batching matters: a block's previous outputs are resolved
    /// one round trip per transaction otherwise, which dominates ingest on any
    /// link with latency. Batches must be JSON-RPC 2.0; the node rejects a
    /// 1.0 batch with -32600, so single calls elsewhere stay on 1.0.
    pub async fn transactions(
        &self,
        txids: &[String],
    ) -> Result<Vec<Option<Transaction>>, ZakuraError> {
        if txids.is_empty() {
            return Ok(Vec::new());
        }
        let requests: Vec<serde_json::Value> = txids
            .iter()
            .enumerate()
            .map(|(index, txid)| {
                // Batches must be JSON-RPC 2.0: the node rejects a 1.0 batch
                // with -32600. Single calls elsewhere stay on 1.0, matching
                // the sibling coordinator and the zcashd convention.
                json!({
                    "jsonrpc": "2.0",
                    "id": index,
                    "method": "getrawtransaction",
                    "params": [txid, 0],
                })
            })
            .collect();
        let response = self
            .http
            .post(&self.rpc_url)
            .basic_auth(&self.username, Some(&self.password))
            .json(&requests)
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(ZakuraError::InvalidCookie);
        }
        let entries: Vec<BatchEntry> = response.error_for_status()?.json().await?;
        if entries.len() != txids.len() {
            return Err(ZakuraError::Batch(format!(
                "asked for {} transactions, got {}",
                txids.len(),
                entries.len()
            )));
        }
        // A batch response may arrive in any order, so results are placed by
        // their echoed id rather than by position.
        let mut out: Vec<Option<Transaction>> = vec![None; txids.len()];
        let mut seen = vec![false; txids.len()];
        for entry in entries {
            let index = entry
                .id
                .ok_or_else(|| ZakuraError::Batch("batch response entry has no id".to_string()))?;
            let index = usize::try_from(index)
                .ok()
                .filter(|index| *index < txids.len())
                .ok_or_else(|| ZakuraError::Batch(format!("batch id {index} is out of range")))?;
            if std::mem::replace(&mut seen[index], true) {
                return Err(ZakuraError::Batch(format!(
                    "batch id {index} appeared twice"
                )));
            }
            if let Some(error) = entry.error {
                // -5 is "No information available about transaction".
                if error.code == -5 {
                    continue;
                }
                return Err(ZakuraError::Rpc(error.code, error.message));
            }
            let raw_hex = entry
                .result
                .ok_or_else(|| ZakuraError::Batch("batch entry has no result".to_string()))?;
            let raw = hex::decode(raw_hex)?;
            out[index] = Some(
                Transaction::zcash_deserialize(raw.as_slice()).map_err(|error| {
                    ZakuraError::Transaction(txids[index].clone(), error.to_string())
                })?,
            );
        }
        if seen.iter().any(|seen| !seen) {
            return Err(ZakuraError::Batch(
                "batch response is missing an entry".to_string(),
            ));
        }
        Ok(out)
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, ZakuraError> {
        let response = self
            .http
            .post(&self.rpc_url)
            .basic_auth(&self.username, Some(&self.password))
            .json(&json!({
                "jsonrpc": "1.0",
                "id": "transparent-filter",
                "method": method,
                "params": params
            }))
            .send()
            .await?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED {
            return Err(ZakuraError::InvalidCookie);
        }
        let response: RpcResponse<T> = response.error_for_status()?.json().await?;
        if let Some(error) = response.error {
            return Err(ZakuraError::Rpc(error.code, error.message));
        }
        response.result.ok_or(ZakuraError::MissingResult)
    }
}
