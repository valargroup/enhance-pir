use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u16 = 1;
pub const NETWORK: &str = "main";
pub const POOL: &str = "ironwood";
pub const ACTIVATION_HEIGHT: u64 = 3_428_143;
pub const CONFIRMATIONS: u64 = 10;
pub const RECORD_BYTES: usize = 612;
pub const RECORDS_PER_ROW: usize = 8;
pub const ROW_BYTES: usize = RECORD_BYTES * RECORDS_PER_ROW;
pub const SHARD_ROWS: usize = 8_192;
pub const SHARD_POSITIONS: usize = SHARD_ROWS * RECORDS_PER_ROW;
/// Fixed placement quantum. Worker `n` owns shard IDs `n * 2..(n + 1) * 2`.
/// Appending workers therefore never moves an already-published shard.
pub const SHARDS_PER_WORKER: u64 = 2;
pub const ITEM_SIZE_BITS: u64 = (ROW_BYTES * 8) as u64;
pub const DEFAULT_LOOKBACK_BLOCKS: u64 = 210_240;
pub const DEFAULT_MAX_ACTIVE_SHARDS: u32 = 16;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoRecord(pub [u8; RECORD_BYTES]);

impl MemoRecord {
    pub fn from_parts(ephemeral_key: [u8; 32], ciphertext: [u8; 580]) -> Self {
        let mut bytes = [0; RECORD_BYTES];
        bytes[..32].copy_from_slice(&ephemeral_key);
        bytes[32..].copy_from_slice(&ciphertext);
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; RECORD_BYTES] {
        &self.0
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Coverage {
    Full {
        covered_position_start: u64,
    },
    Windowed {
        requested_lookback_blocks: u64,
        max_active_shards: u32,
        covered_position_start: u64,
        effective_start_height: u64,
    },
}

impl Coverage {
    pub fn covered_position_start(&self) -> u64 {
        match self {
            Self::Full {
                covered_position_start,
            }
            | Self::Windowed {
                covered_position_start,
                ..
            } => *covered_position_start,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardDescriptor {
    pub shard_id: u64,
    pub global_row_start: u64,
    pub populated_positions: u64,
    pub rows_sha256: String,
    pub sealed: bool,
    pub worker: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemoSnapshotMetadata {
    pub schema_version: u16,
    pub network: String,
    pub pool: String,
    pub anchor_height: u64,
    pub anchor_block_hash: String,
    pub ironwood_tree_size: u64,
    pub coverage: Coverage,
    pub record_bytes: u32,
    pub records_per_row: u32,
    pub row_bytes: u32,
    pub shard_rows: u32,
    pub used_rows: u64,
    pub logical_rows: u64,
    pub first_global_row: u64,
    pub generation: u64,
    pub parameter_id: String,
    pub setup_seed: u64,
    pub public_params_epoch: String,
    pub public_params_sha256: String,
    pub shards: Vec<ShardDescriptor>,
}

impl MemoSnapshotMetadata {
    pub fn local_row_for_position(&self, position: u64) -> Option<(usize, usize)> {
        if position < self.coverage.covered_position_start() || position >= self.ironwood_tree_size
        {
            return None;
        }
        let global_row = position / RECORDS_PER_ROW as u64;
        if global_row >= self.logical_rows {
            return None;
        }
        Some((global_row as usize, position as usize % RECORDS_PER_ROW))
    }
}

pub fn logical_rows_for(used_rows: u64) -> u64 {
    used_rows.max(SHARD_ROWS as u64).next_power_of_two()
}

pub fn worker_index_for_shard(shard_id: u64, worker_count: usize) -> Option<usize> {
    if worker_count == 0 {
        return None;
    }
    if worker_count == 1 {
        return Some(0);
    }
    let index = usize::try_from(shard_id / SHARDS_PER_WORKER).ok()?;
    (index < worker_count).then_some(index)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geometry_is_fixed_and_aligned() {
        assert_eq!(ROW_BYTES, 4_896);
        assert_eq!(SHARD_POSITIONS, 65_536);
        assert_eq!(SHARD_ROWS % 2_048, 0);
        assert_eq!(logical_rows_for(0), 8_192);
        assert_eq!(logical_rows_for(16_819), 32_768);
    }

    #[test]
    fn record_layout_is_ephemeral_key_then_full_ciphertext() {
        let record = MemoRecord::from_parts([1; 32], [2; 580]);
        assert_eq!(&record.as_bytes()[..32], &[1; 32]);
        assert_eq!(&record.as_bytes()[32..], &[2; 580]);
    }

    #[test]
    fn adding_workers_does_not_move_sealed_shards() {
        for shard in 0..4 {
            assert_eq!(worker_index_for_shard(shard, 2), Some((shard / 2) as usize));
            assert_eq!(worker_index_for_shard(shard, 3), Some((shard / 2) as usize));
        }
        assert_eq!(worker_index_for_shard(4, 2), None);
        assert_eq!(worker_index_for_shard(4, 3), Some(2));
    }
}
