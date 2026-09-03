//! Server-internal geometry adapters for the single Enhance PIR database.

pub use enhance_pir::types::{
    logical_rows_for, setup_seed_bytes, used_rows_for, worker_index_for_shard, EnhanceGeneration,
    EnhanceRecord, EnhanceRecordParts, ShardDescriptor, ACTIVATION_HEIGHT, CONFIRMATIONS,
    ENHANCE_SETUP_SEED, ITEM_SIZE_BITS, NETWORK, POOL, PROTOCOL_REVISION, RECORDS_PER_ROW,
    RECORD_BYTES, ROW_BYTES, SCHEMA_VERSION, SHARDS_PER_WORKER, SHARD_POSITIONS, SHARD_ROWS,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum DatabaseId {
    Enhance,
}

impl DatabaseId {
    pub const ALL: [Self; 1] = [Self::Enhance];

    pub const fn as_str(self) -> &'static str {
        "enhance"
    }

    pub const fn layout(self) -> DatabaseLayout {
        ENHANCE_LAYOUT
    }

    pub const fn setup_seed(self) -> u64 {
        ENHANCE_SETUP_SEED
    }
}

impl std::fmt::Display for DatabaseId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl std::str::FromStr for DatabaseId {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        (value == "enhance")
            .then_some(Self::Enhance)
            .ok_or_else(|| format!("unknown PIR database: {value:?}"))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DatabaseLayout {
    pub record_bytes: usize,
    pub records_per_row: usize,
    pub shard_rows: usize,
}

impl DatabaseLayout {
    pub const fn row_bytes(self) -> usize {
        self.record_bytes * self.records_per_row
    }

    pub const fn shard_positions(self) -> usize {
        self.shard_rows * self.records_per_row
    }

    pub const fn item_size_bits(self) -> u64 {
        (self.row_bytes() * 8) as u64
    }

    pub const fn shard_bytes(self) -> usize {
        self.shard_rows * self.row_bytes()
    }

    pub const fn used_rows_for(self, positions: u64) -> u64 {
        positions.div_ceil(self.records_per_row as u64)
    }

    pub fn logical_rows_for(self, used_rows: u64) -> u64 {
        used_rows.max(self.shard_rows as u64).next_power_of_two()
    }
}

pub const ENHANCE_LAYOUT: DatabaseLayout = DatabaseLayout {
    record_bytes: RECORD_BYTES,
    records_per_row: RECORDS_PER_ROW,
    shard_rows: SHARD_ROWS,
};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TableManifest {
    pub record_bytes: u32,
    pub records_per_row: u32,
    pub row_bytes: u32,
    pub shard_rows: u32,
    pub positions: u64,
    pub used_rows: u64,
    pub logical_rows: u64,
    pub parameter_id: String,
    pub setup_seed: u64,
    pub public_params_epoch: String,
    pub public_params_sha256: String,
    pub shards: Vec<ShardDescriptor>,
}

/// Internal representation retained while the coordinator manages generations.
#[derive(Clone, Debug)]
pub struct GenerationManifest {
    pub anchor_height: u64,
    pub anchor_block_hash: String,
    pub ironwood_tree_size: u64,
    pub generation: u64,
    pub tables: BTreeMap<DatabaseId, TableManifest>,
}

impl GenerationManifest {
    pub fn public(&self) -> Option<EnhanceGeneration> {
        let table = self.tables.get(&DatabaseId::Enhance)?;
        Some(EnhanceGeneration {
            schema_version: SCHEMA_VERSION,
            protocol_revision: PROTOCOL_REVISION.to_string(),
            network: NETWORK.to_string(),
            pool: POOL.to_string(),
            anchor_height: self.anchor_height,
            anchor_block_hash: self.anchor_block_hash.clone(),
            ironwood_tree_size: self.ironwood_tree_size,
            generation: self.generation,
            record_bytes: table.record_bytes,
            records_per_row: table.records_per_row,
            row_bytes: table.row_bytes,
            shard_rows: table.shard_rows,
            used_rows: table.used_rows,
            logical_rows: table.logical_rows,
            parameter_id: table.parameter_id.clone(),
            setup_seed: table.setup_seed,
            public_params_epoch: table.public_params_epoch.clone(),
            public_params_sha256: table.public_params_sha256.clone(),
            shards: table.shards.clone(),
        })
    }
}
