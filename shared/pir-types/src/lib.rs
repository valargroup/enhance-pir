//! Shared PIR types used by both the nullifier and witness subsystems.
//!
//! Contains the [`PirEngine`] trait (abstracting over YPIR vs stub),
//! YPIR scenario parameters, server lifecycle phases, and chain
//! constants shared across all PIR services.

use serde::{Deserialize, Serialize};

/// Blocks behind the tip at which the PIR server anchors its database state.
/// Shared by both nullifier and witness PIR servers. Deep enough (10) to survive
/// typical reorgs while still being fresh enough for practical spending.
pub const CONFIRMATION_DEPTH: u64 = 10;

/// Mainnet activation height for NU6.3, which instantiates the Ironwood value
/// pool. No Ironwood data exists below this height, so PIR ingest starts here.
pub const NU6_3_MAINNET_ACTIVATION: u64 = 3_428_143;

/// Testnet activation height for NU6.3.
pub const NU6_3_TESTNET_ACTIVATION: u64 = 4_134_000;

/// The shielded pool these PIR databases cover.
///
/// Reported on the `/metadata` endpoints and checked by clients, so a wallet
/// cannot take Orchard answers for Ironwood questions. On-disk artifacts carry
/// their own format versions (`hashtable-pir`'s `SNAPSHOT_VERSION` and
/// `commitment-tree-db`'s snapshot magic), which is what actually fences
/// Orchard-era files.
pub const POOL: &str = "ironwood";

/// The Zcash network a PIR server is following.
///
/// Determined at bootstrap from the `network` field of a lightwalletd
/// `TreeState` response rather than from local configuration, so a server can
/// never disagree with the chain it is actually reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ZcashNetwork {
    /// Mainnet.
    Main,
    /// Testnet.
    Test,
}

impl ZcashNetwork {
    /// The NU6.3 activation height for this network — the lowest block that can
    /// contain Ironwood data.
    pub const fn activation_height(&self) -> u64 {
        match self {
            ZcashNetwork::Main => NU6_3_MAINNET_ACTIVATION,
            ZcashNetwork::Test => NU6_3_TESTNET_ACTIVATION,
        }
    }

    /// Parse the `network` field of a lightwalletd `TreeState` / `LightdInfo`
    /// response ("main" or "test"). Returns `None` for anything else, including
    /// regtest, which has no fixed activation height.
    pub fn from_lwd_name(name: &str) -> Option<Self> {
        match name {
            "main" => Some(ZcashNetwork::Main),
            "test" => Some(ZcashNetwork::Test),
            _ => None,
        }
    }

    /// The name lightwalletd uses for this network.
    pub const fn as_lwd_name(&self) -> &'static str {
        match self {
            ZcashNetwork::Main => "main",
            ZcashNetwork::Test => "test",
        }
    }
}

/// Lowest block height a PIR server will ever sync.
///
/// Normally the network's NU6.3 activation height. Short test chains whose tip
/// has not reached activation fall back to height 1 so that local harnesses and
/// regtest still work.
pub fn min_sync_height(network: Option<ZcashNetwork>, tip_height: u64) -> u64 {
    match network {
        Some(network) if tip_height >= network.activation_height() => network.activation_height(),
        _ => 1,
    }
}

/// Public setup seed shared by IPIR-SP clients and servers.
///
/// The seed is not secret; it deterministically derives server-side public
/// setup material so the existing `/params` endpoint can remain unchanged.
pub const IPIR_SETUP_SEED: [u8; 32] = *b"spendability-pir-ipir-setup-0000";

/// Server lifecycle phase, reported via `/metadata` endpoints.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ServerPhase {
    /// Catching up to the chain tip during initial sync.
    Syncing {
        current_height: u64,
        target_height: u64,
    },
    /// Fully synced and serving PIR queries.
    Serving,
}

/// SimplePIR scenario parameters describing the database geometry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct YpirScenario {
    /// Number of rows in the PIR database.
    pub num_items: u64,
    /// Size of each row in bits.
    pub item_size_bits: u64,
}

/// Abstraction over the PIR engine, allowing stub implementations for testing
/// and the real YPIR engine in production.
pub trait PirEngine: Send + Sync {
    type ServerState: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    /// Offline precomputation: build server state from raw DB bytes and scenario.
    fn setup(
        &self,
        db_bytes: &[u8],
        scenario: &YpirScenario,
    ) -> Result<Self::ServerState, Self::Error>;

    /// Online computation: answer a single encrypted client query.
    fn answer_query(
        &self,
        state: &Self::ServerState,
        query_bytes: &[u8],
    ) -> Result<Vec<u8>, Self::Error>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn server_phase_serde_roundtrip() {
        let syncing = ServerPhase::Syncing {
            current_height: 100,
            target_height: 200,
        };
        let json = serde_json::to_string(&syncing).unwrap();
        let decoded: ServerPhase = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, syncing);

        let serving = ServerPhase::Serving;
        let json = serde_json::to_string(&serving).unwrap();
        let decoded: ServerPhase = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded, serving);
    }

    #[test]
    fn network_parses_lwd_names() {
        assert_eq!(
            ZcashNetwork::from_lwd_name("main"),
            Some(ZcashNetwork::Main)
        );
        assert_eq!(
            ZcashNetwork::from_lwd_name("test"),
            Some(ZcashNetwork::Test)
        );
        assert_eq!(ZcashNetwork::from_lwd_name("regtest"), None);
    }

    #[test]
    fn min_sync_height_uses_activation_or_falls_back() {
        // Post-activation mainnet tip: start at NU6.3.
        assert_eq!(
            min_sync_height(Some(ZcashNetwork::Main), 3_469_040),
            NU6_3_MAINNET_ACTIVATION
        );
        // A tip below activation (or an unknown network) means a short test
        // chain, so sync everything.
        assert_eq!(min_sync_height(Some(ZcashNetwork::Main), 500), 1);
        assert_eq!(min_sync_height(None, 3_469_040), 1);
        assert_eq!(
            min_sync_height(Some(ZcashNetwork::Test), 4_200_000),
            NU6_3_TESTNET_ACTIVATION
        );
    }

    #[test]
    fn ypir_scenario_serde_roundtrip() {
        let scenario = YpirScenario {
            num_items: 16_384,
            item_size_bits: 28_672,
        };
        let json = serde_json::to_string(&scenario).unwrap();
        let decoded: YpirScenario = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.num_items, 16_384);
        assert_eq!(decoded.item_size_bits, 28_672);
    }
}
