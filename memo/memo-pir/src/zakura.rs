use crate::types::{ActionRecord, ActionRecordParts};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use std::path::Path;
use zakura_chain::block::Block;
use zakura_chain::serialization::ZcashDeserialize;

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
    #[error("invalid block hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("invalid canonical block: {0}")]
    Block(String),
    #[error("Ironwood tree size is unavailable at height {0}")]
    MissingTreeSize(u64),
}

#[derive(Clone)]
pub struct ZakuraClient {
    http: reqwest::Client,
    rpc_url: String,
    username: String,
    password: String,
}

/// One transaction's Ironwood actions, in consensus order.
#[derive(Debug, Clone)]
pub struct CanonicalTx {
    /// Transaction ID in internal byte order.
    pub txid: [u8; 32],
    /// Index of this transaction's first action within the block's actions.
    pub first_action_index: usize,
    pub nullifiers: Vec<[u8; 32]>,
    pub cmxs: Vec<[u8; 32]>,
}

impl CanonicalTx {
    pub fn action_count(&self) -> usize {
        self.cmxs.len()
    }
}

/// A finalized block as every table's ingest sees it: the full action records
/// in consensus order, the per-transaction boundaries, and the Ironwood tree
/// size after the block.
#[derive(Debug)]
pub struct CanonicalBlock {
    pub height: u64,
    pub hash: String,
    pub records: Vec<ActionRecord>,
    pub transactions: Vec<CanonicalTx>,
    pub tree_size: u64,
}

impl CanonicalBlock {
    /// Tree size before the block: the position of its first action.
    pub fn first_position(&self) -> Option<u64> {
        self.tree_size.checked_sub(self.records.len() as u64)
    }
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Deserialize)]
struct VerboseBlock {
    trees: BlockTrees,
}

#[derive(Deserialize)]
struct BlockTrees {
    ironwood: Option<TreeSize>,
}

#[derive(Deserialize)]
struct TreeSize {
    size: u64,
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

    pub async fn tree_size(&self, height: u64) -> Result<u64, ZakuraError> {
        let block: VerboseBlock = self
            .call("getblock", json!([height.to_string(), 2]))
            .await?;
        block
            .trees
            .ironwood
            .map(|tree| tree.size)
            .ok_or(ZakuraError::MissingTreeSize(height))
    }

    pub async fn block(&self, height: u64) -> Result<CanonicalBlock, ZakuraError> {
        let raw_hex: String = self
            .call("getblock", json!([height.to_string(), 0]))
            .await?;
        let raw = hex::decode(raw_hex)?;
        let block = Block::zcash_deserialize(raw.as_slice())
            .map_err(|error| ZakuraError::Block(error.to_string()))?;
        let hash = block.hash().to_string();
        let record_height = u32::try_from(height)
            .map_err(|_| ZakuraError::Block(format!("height {height} exceeds u32")))?;
        let mut records = Vec::new();
        let mut transactions = Vec::new();
        for transaction in &block.transactions {
            let txid = <[u8; 32]>::from(transaction.hash());
            let first_action_index = records.len();
            let mut nullifiers = Vec::new();
            let mut cmxs = Vec::new();
            for action in transaction.ironwood_actions() {
                let nullifier: [u8; 32] = action.nullifier.into();
                let cmx = <[u8; 32]>::from(action.cm_x);
                records.push(ActionRecord::from_parts(ActionRecordParts {
                    nullifier,
                    ephemeral_key: <[u8; 32]>::from(&action.ephemeral_key),
                    enc_ciphertext: action.enc_ciphertext.into(),
                    cmx,
                    cv_net: action.cv.into(),
                    out_ciphertext: action.out_ciphertext.into(),
                    txid,
                    height: record_height,
                }));
                nullifiers.push(nullifier);
                cmxs.push(cmx);
            }
            if !cmxs.is_empty() {
                transactions.push(CanonicalTx {
                    txid,
                    first_action_index,
                    nullifiers,
                    cmxs,
                });
            }
        }
        let tree_size = self.tree_size(height).await?;
        Ok(CanonicalBlock {
            height,
            hash,
            records,
            transactions,
            tree_size,
        })
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
                "id": "memo-pir",
                "method": method,
                "params": params,
            }))
            .send()
            .await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            return Err(ZakuraError::InvalidCookie);
        }
        let response: RpcResponse<T> = response.error_for_status()?.json().await?;
        if let Some(error) = response.error {
            return Err(ZakuraError::Rpc(error.code, error.message));
        }
        response.result.ok_or(ZakuraError::MissingResult)
    }
}
