//! Table identities, row layouts, and the generation manifest shared by the
//! PIR coordinator, its workers, and wallet clients.
//!
//! Everything here is protocol: a change to a layout, a seed, or a wire name
//! is a schema version bump for every party at once.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Wire schema of [`GenerationManifest`].
pub const MANIFEST_SCHEMA_VERSION: u16 = 4;

/// The pinned `ipir-sp` revision every party derives parameters from.
pub const PROTOCOL_REVISION: &str = "ipir-sp-e875404";

/// Row geometry of one PIR table. Every table the coordinator serves has its
/// own layout; the iPIR parameters, shard sizes, and artifact shapes derive
/// from it, so it is the one value that must agree between ingest, workers,
/// coordinator, and clients.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct DatabaseLayout {
    /// Bytes in one logical record.
    pub record_bytes: usize,
    /// Records packed into one PIR row.
    pub records_per_row: usize,
    /// Rows in one independently published shard. Must be a multiple of the
    /// RLWE degree so a shard's setup polynomials are a contiguous slice.
    pub shard_rows: usize,
}

impl DatabaseLayout {
    pub const fn row_bytes(&self) -> usize {
        self.record_bytes * self.records_per_row
    }

    pub const fn shard_positions(&self) -> usize {
        self.shard_rows * self.records_per_row
    }

    /// iPIR item size for one row.
    pub const fn item_size_bits(&self) -> u64 {
        (self.row_bytes() * 8) as u64
    }

    pub const fn shard_bytes(&self) -> usize {
        self.shard_rows * self.row_bytes()
    }

    pub fn used_rows_for(&self, positions: u64) -> u64 {
        positions.div_ceil(self.records_per_row as u64)
    }

    /// Public capacity: a power of two, never below one shard, so growth only
    /// extends the prefix-stable setup and sealed shards keep their CRS.
    pub fn logical_rows_for(&self, used_rows: u64) -> u64 {
        used_rows.max(self.shard_rows as u64).next_power_of_two()
    }

    pub fn row_for_position(&self, position: u64) -> (u64, usize) {
        (
            position / self.records_per_row as u64,
            (position % self.records_per_row as u64) as usize,
        )
    }
}

/// Identity of one PIR table. The wire name (`as_str`) appears in URLs,
/// artifact paths, and the generation manifest, so it is fixed forever.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DatabaseId {
    /// One Ironwood action per record, eight per row, indexed by position.
    Action,
    /// Note commitments, 256 per row: one sub-shard per row, indexed by
    /// sub-shard (tree levels 0 to 8).
    Witness,
    /// Completed sub-shard roots, 256 per row: one shard per row, indexed by
    /// shard (tree levels 8 to 16). The frontier sub-shard's root is public
    /// and travels in the broadcast cap instead.
    WitnessRoots,
    /// Nullifier hash buckets up to the cold checkpoint.
    NfCold,
    /// Nullifier hash buckets since the cold checkpoint.
    NfWarm,
}

impl DatabaseId {
    pub const ALL: [DatabaseId; 5] = [
        DatabaseId::Action,
        DatabaseId::Witness,
        DatabaseId::WitnessRoots,
        DatabaseId::NfCold,
        DatabaseId::NfWarm,
    ];

    pub const fn as_str(&self) -> &'static str {
        match self {
            DatabaseId::Action => "action",
            DatabaseId::Witness => "witness",
            DatabaseId::WitnessRoots => "witness-roots",
            DatabaseId::NfCold => "nf-cold",
            DatabaseId::NfWarm => "nf-warm",
        }
    }

    /// Row geometry of the table.
    pub const fn layout(&self) -> DatabaseLayout {
        match self {
            DatabaseId::Action => ACTION_LAYOUT,
            DatabaseId::Witness | DatabaseId::WitnessRoots => WITNESS_LAYOUT,
            DatabaseId::NfCold | DatabaseId::NfWarm => NULLIFIER_LAYOUT,
        }
    }

    /// Seed of the table's deterministic public query setup. ACTION keeps the
    /// original memo-PIR seed so sealed artifacts and pinned clients stay
    /// valid; every other table is domain-separated by its wire name.
    pub fn setup_seed(&self) -> u64 {
        match self {
            DatabaseId::Action => MEMO_SETUP_SEED,
            other => seed_from_domain(&format!(
                "zcash/ironwood-pir/{}/setup-seed/v1",
                other.as_str()
            )),
        }
    }
}

impl std::fmt::Display for DatabaseId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl std::str::FromStr for DatabaseId {
    type Err = String;

    fn from_str(name: &str) -> Result<Self, Self::Err> {
        DatabaseId::ALL
            .into_iter()
            .find(|id| id.as_str() == name)
            .ok_or_else(|| format!("unknown PIR table: {name:?}"))
    }
}

/// The ACTION table: one Ironwood action per record, eight per row.
/// Record v3 (824 bytes) adds `cmx` so an unknown note can be authenticated
/// after trial decryption without any other lookup.
pub const ACTION_LAYOUT: DatabaseLayout = DatabaseLayout {
    record_bytes: 824,
    records_per_row: 8,
    shard_rows: 8_192,
};

/// The WITNESS table: one note commitment per record, one sub-shard of 256
/// leaves per row. Provisional until the witness table ships.
pub const WITNESS_LAYOUT: DatabaseLayout = DatabaseLayout {
    record_bytes: 32,
    records_per_row: 256,
    shard_rows: 8_192,
};

/// The nullifier tables: one hash bucket per row, 112 entries of 41 bytes.
/// Provisional until the nullifier tables ship.
pub const NULLIFIER_LAYOUT: DatabaseLayout = DatabaseLayout {
    record_bytes: 4_592,
    records_per_row: 1,
    shard_rows: 8_192,
};

/// First eight bytes, little-endian, of
/// `SHA-256("zcash/ironwood-memo-pir/setup-seed/v1")`: the ACTION setup seed.
pub const MEMO_SETUP_SEED: u64 = 0xaf1a_e284_ec07_131a;

/// First eight little-endian bytes of the SHA-256 of a domain string.
pub fn seed_from_domain(domain: &str) -> u64 {
    use sha2::{Digest, Sha256};
    let digest = Sha256::digest(domain.as_bytes());
    u64::from_le_bytes(digest[..8].try_into().expect("eight bytes"))
}

/// Expands a `u64` seed into the 32-byte seed the setup generator takes: the
/// value in the low eight bytes, zero elsewhere. Byte-identical to the
/// nullifier-PIR expansion so the two deployments cannot share a setup only
/// by choosing the same `u64`.
pub fn setup_seed_bytes(seed: u64) -> [u8; 32] {
    let mut bytes = [0; 32];
    bytes[..8].copy_from_slice(&seed.to_le_bytes());
    bytes
}

/// Fixed per-pass query budget every wallet issues, dummies included, so the
/// request count never depends on what a wallet found. Changing it is a
/// protocol version for every wallet at once.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Envelope {
    pub protocol_version: u16,
    /// Nullifier query pairs (one cold, one warm) per pass.
    pub k_nf: u16,
    /// ACTION row queries per pass.
    pub k_act: u16,
    /// Witness query pairs (one roots row, one leaves row) per pass.
    pub k_wit: u16,
}

/// The envelope this protocol version publishes.
pub const DEFAULT_ENVELOPE: Envelope = Envelope {
    protocol_version: 1,
    k_nf: 8,
    k_act: 4,
    k_wit: 4,
};

/// Blocks between cold nullifier checkpoints. The cold table is rebuilt
/// only when the checkpoint moves.
pub const COLD_CHECKPOINT_INTERVAL: u64 = 1_000;

/// One published shard of one table.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardDescriptor {
    pub shard_id: u64,
    pub global_row_start: u64,
    pub populated_positions: u64,
    pub rows_sha256: String,
    pub sealed: bool,
    pub worker: String,
}

/// One table as published in a generation.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableManifest {
    pub record_bytes: u32,
    pub records_per_row: u32,
    pub row_bytes: u32,
    pub shard_rows: u32,
    /// Populated positions (records) in the table.
    pub positions: u64,
    pub used_rows: u64,
    pub logical_rows: u64,
    pub parameter_id: String,
    pub setup_seed: u64,
    pub public_params_epoch: String,
    pub public_params_sha256: String,
    pub shards: Vec<ShardDescriptor>,
}

/// Everything a client needs to know about one generation of every table,
/// served at `GET /v1/generation`. A client pins one generation for a whole
/// DAG-sync pass and the coordinator keeps two answerable.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct GenerationManifest {
    pub schema_version: u16,
    pub protocol_revision: String,
    pub network: String,
    pub pool: String,
    pub anchor_height: u64,
    /// Hex, display byte order.
    pub anchor_block_hash: String,
    pub ironwood_tree_size: u64,
    pub generation: u64,
    /// Depth-32 root of the Ironwood commitment tree at the anchor, hex, so a
    /// client can bind witness paths to the block it scanned.
    pub anchor_tree_root: String,
    /// Height of the nullifier cold checkpoint this generation was built from.
    pub cold_checkpoint_height: u64,
    pub envelope: Envelope,
    pub tables: BTreeMap<DatabaseId, TableManifest>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn layout_derivations_match_the_action_constants() {
        assert_eq!(ACTION_LAYOUT.row_bytes(), 6_592);
        assert_eq!(ACTION_LAYOUT.shard_positions(), 65_536);
        assert_eq!(ACTION_LAYOUT.item_size_bits(), 52_736);
        assert_eq!(ACTION_LAYOUT.shard_bytes(), 54_001_664);
        assert_eq!(ACTION_LAYOUT.used_rows_for(0), 0);
        assert_eq!(ACTION_LAYOUT.used_rows_for(9), 2);
        assert_eq!(ACTION_LAYOUT.logical_rows_for(0), 8_192);
        assert_eq!(ACTION_LAYOUT.logical_rows_for(16_819), 32_768);
        assert_eq!(ACTION_LAYOUT.row_for_position(65_537), (8_192, 1));
        for table in DatabaseId::ALL {
            assert!(table.layout().shard_rows.is_multiple_of(2_048), "{table}");
        }
    }

    #[test]
    fn table_names_are_fixed_and_round_trip() {
        for id in DatabaseId::ALL {
            assert_eq!(id.as_str().parse::<DatabaseId>().unwrap(), id);
            assert_eq!(
                serde_json::to_string(&id).unwrap(),
                format!("{:?}", id.as_str())
            );
        }
        assert_eq!(DatabaseId::NfCold.as_str(), "nf-cold");
        assert!("memo".parse::<DatabaseId>().is_err());
        // Map keys serialize as the wire names.
        let mut tables = BTreeMap::new();
        tables.insert(DatabaseId::NfWarm, 1u8);
        assert_eq!(serde_json::to_string(&tables).unwrap(), r#"{"nf-warm":1}"#);
    }

    #[test]
    fn setup_seeds_are_domain_separated_and_action_is_pinned() {
        assert_eq!(
            seed_from_domain("zcash/ironwood-memo-pir/setup-seed/v1"),
            MEMO_SETUP_SEED
        );
        assert_eq!(DatabaseId::Action.setup_seed(), MEMO_SETUP_SEED);
        let mut seeds: Vec<u64> = DatabaseId::ALL.iter().map(DatabaseId::setup_seed).collect();
        seeds.sort_unstable();
        seeds.dedup();
        assert_eq!(seeds.len(), DatabaseId::ALL.len());
        assert_eq!(
            &setup_seed_bytes(MEMO_SETUP_SEED)[..8],
            &MEMO_SETUP_SEED.to_le_bytes()
        );
        assert!(setup_seed_bytes(MEMO_SETUP_SEED)[8..]
            .iter()
            .all(|byte| *byte == 0));
    }
}
