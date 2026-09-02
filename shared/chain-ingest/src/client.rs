//! Lightwalletd gRPC client wrapper.
//!
//! Provides [`LwdClient`] for connecting to a lightwalletd instance and
//! fetching compact blocks and chain tip information.

use crate::proto::compact_tx_streamer_client::CompactTxStreamerClient;
use crate::proto::{
    BlockId, BlockRange, ChainSpec, CompactBlock, Empty, GetSubtreeRootsArg, SubtreeRoot, TreeState,
};
use pir_types::ZcashNetwork;
use thiserror::Error;
use tokio_stream::StreamExt;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("failed to connect to any endpoint")]
    NoEndpointAvailable,
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
    #[error("gRPC status error: {0}")]
    Status(#[from] tonic::Status),
    #[error(
        "lightwalletd omitted the Ironwood commitment tree at height {height} on {network}, \
         which is past NU6.3 activation ({activation}); use a post-NU6.3 endpoint"
    )]
    MissingIronwoodTree {
        network: &'static str,
        height: u64,
        activation: u64,
    },
}

pub type Result<T> = std::result::Result<T, ClientError>;

/// Wrapper around the lightwalletd gRPC client.
pub struct LwdClient {
    inner: CompactTxStreamerClient<Channel>,
}

impl LwdClient {
    /// Connect to the first reachable endpoint from the given list.
    /// Automatically enables TLS for `https://` endpoints.
    pub async fn connect(endpoints: &[String]) -> Result<Self> {
        for endpoint in endpoints {
            let result = if endpoint.starts_with("https://") {
                let tls = ClientTlsConfig::new().with_native_roots();
                let ep = Endpoint::from_shared(endpoint.clone())?.tls_config(tls)?;
                CompactTxStreamerClient::connect(ep).await
            } else {
                CompactTxStreamerClient::connect(endpoint.clone()).await
            };

            match result {
                Ok(client) => {
                    tracing::info!(endpoint, "connected to lightwalletd");
                    return Ok(Self { inner: client });
                }
                Err(e) => {
                    tracing::warn!(endpoint, error = %e, "failed to connect, trying next");
                }
            }
        }
        Err(ClientError::NoEndpointAvailable)
    }

    /// Wrap an existing tonic channel (useful for testing with mock servers).
    pub fn from_channel(channel: Channel) -> Self {
        Self {
            inner: CompactTxStreamerClient::new(channel),
        }
    }

    /// Get the height and hash of the chain tip.
    pub async fn get_latest_block(&mut self) -> Result<(u64, Vec<u8>)> {
        let response = self.inner.get_latest_block(ChainSpec {}).await?;
        let block_id = response.into_inner();
        Ok((block_id.height, block_id.hash))
    }

    /// Stream compact blocks in the given range (inclusive).
    pub async fn get_block_range(&mut self, start: u64, end: u64) -> Result<Vec<CompactBlock>> {
        let range = BlockRange {
            start: Some(BlockId {
                height: start,
                hash: vec![],
            }),
            end: Some(BlockId {
                height: end,
                hash: vec![],
            }),
            pool_types: vec![],
        };

        let response = self.inner.get_block_range(range).await?;
        let mut stream = response.into_inner();
        let mut blocks = Vec::new();

        while let Some(block) = stream.next().await {
            blocks.push(block?);
        }

        Ok(blocks)
    }

    /// Get the tree state (frontier) at a specific block height.
    pub async fn get_tree_state(&mut self, height: u64) -> Result<TreeState> {
        let response = self
            .inner
            .get_tree_state(BlockId {
                height,
                hash: vec![],
            })
            .await?;
        Ok(response.into_inner())
    }

    /// Get the latest tree state (at the chain tip).
    pub async fn get_latest_tree_state(&mut self) -> Result<TreeState> {
        let response = self.inner.get_latest_tree_state(Empty {}).await?;
        Ok(response.into_inner())
    }

    /// Determine which network this endpoint follows, and verify that it
    /// actually serves Ironwood data.
    ///
    /// The network is read from the `network` field of the latest `TreeState`
    /// rather than taken from local configuration, so a server can never
    /// disagree with the chain it is reading. Returns `Ok(None)` for a network
    /// name we do not recognise (regtest, local harnesses), in which case
    /// callers should sync from height 1.
    ///
    /// Fails with [`ClientError::MissingIronwoodTree`] if the endpoint is on a
    /// known network past NU6.3 activation but reports an empty Ironwood tree.
    /// A pre-v0.5.0 lightwalletd does exactly that, and without this check it
    /// would silently yield an empty PIR database.
    ///
    /// An endpoint that does not implement `GetLatestTreeState` at all is
    /// treated as an unrecognised network, not an error: that is a test
    /// harness, not a stale lightwalletd. The distinction that matters is
    /// "could not ask" versus "answered, and the answer was wrong" — a real
    /// pre-Ironwood server implements the RPC and is still caught below.
    pub async fn detect_ironwood_network(&mut self) -> Result<Option<ZcashNetwork>> {
        let state = match self.get_latest_tree_state().await {
            Ok(state) => state,
            Err(ClientError::Status(status)) if status.code() == tonic::Code::Unimplemented => {
                tracing::warn!(
                    "endpoint does not implement GetLatestTreeState; \
                     treating as a short test chain"
                );
                return Ok(None);
            }
            Err(e) => return Err(e),
        };
        let Some(network) = ZcashNetwork::from_lwd_name(&state.network) else {
            tracing::warn!(
                network = %state.network,
                "unrecognised lightwalletd network; treating as a short test chain"
            );
            return Ok(None);
        };

        let activation = network.activation_height();
        if state.height >= activation && state.ironwood_tree.is_empty() {
            return Err(ClientError::MissingIronwoodTree {
                network: network.as_lwd_name(),
                height: state.height,
                activation,
            });
        }

        Ok(Some(network))
    }

    /// Get completed subtree (shard) roots for a shielded protocol.
    ///
    /// `protocol`: 0 = Sapling, 1 = Orchard, 2 = Ironwood.
    /// Returns roots starting from `start_index`, up to `max_entries`.
    pub async fn get_subtree_roots(
        &mut self,
        protocol: i32,
        start_index: u32,
        max_entries: u32,
    ) -> Result<Vec<SubtreeRoot>> {
        let response = self
            .inner
            .get_subtree_roots(GetSubtreeRootsArg {
                start_index,
                shielded_protocol: protocol,
                max_entries,
            })
            .await?;
        let mut stream = response.into_inner();
        let mut roots = Vec::new();
        while let Some(root) = stream.next().await {
            roots.push(root?);
        }
        Ok(roots)
    }
}
