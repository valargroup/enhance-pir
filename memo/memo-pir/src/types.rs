use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u16 = 2;
pub const NETWORK: &str = "main";
pub const POOL: &str = "ironwood";
pub const ACTIVATION_HEIGHT: u64 = 3_428_143;
pub const CONFIRMATIONS: u64 = 10;
/// One Ironwood action record. Layout, in order:
///
/// ```text
/// nf[32] ‖ ephemeralKey[32] ‖ encCiphertext[580] ‖ cv_net[32] ‖ outCiphertext[80] ‖ txid[32] ‖ height[4 LE]
/// ```
///
/// `nf` is the action's spent nullifier, which is `rho` of the output note and
/// is required to trial-decrypt an unknown note. `cv_net` and `outCiphertext`
/// allow outgoing recovery under the OVK. `txid` is in internal (little-endian)
/// byte order. Everything a wallet needs to reconstruct the action except the
/// proof and signature is here; `cm_x` is recomputed from the decrypted note.
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
    Action,
    Witness,
    NfCold,
    NfWarm,
}

impl DatabaseId {
    pub const ALL: [DatabaseId; 4] = [
        DatabaseId::Action,
        DatabaseId::Witness,
        DatabaseId::NfCold,
        DatabaseId::NfWarm,
    ];

    pub const fn as_str(&self) -> &'static str {
        match self {
            DatabaseId::Action => "action",
            DatabaseId::Witness => "witness",
            DatabaseId::NfCold => "nf-cold",
            DatabaseId::NfWarm => "nf-warm",
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
pub const ACTION_LAYOUT: DatabaseLayout = DatabaseLayout {
    record_bytes: 792,
    records_per_row: 8,
    shard_rows: 8_192,
};

// Projections of `ACTION_LAYOUT` kept for the ACTION-specific code paths
// (record parsing, the journal) that are not yet layout-parameterized.
pub const RECORD_BYTES: usize = ACTION_LAYOUT.record_bytes;
pub const RECORDS_PER_ROW: usize = ACTION_LAYOUT.records_per_row;
pub const ROW_BYTES: usize = ACTION_LAYOUT.row_bytes();
pub const SHARD_ROWS: usize = ACTION_LAYOUT.shard_rows;
pub const SHARD_POSITIONS: usize = ACTION_LAYOUT.shard_positions();
/// Fixed placement quantum. Worker `n` owns shard IDs `n * 2..(n + 1) * 2`.
/// Appending workers therefore never moves an already-published shard.
pub const SHARDS_PER_WORKER: u64 = 2;
pub const ITEM_SIZE_BITS: u64 = ACTION_LAYOUT.item_size_bits();
pub const DEFAULT_LOOKBACK_BLOCKS: u64 = 210_240;
pub const DEFAULT_MAX_ACTIVE_SHARDS: u32 = 16;

pub const RECORD_NULLIFIER_OFFSET: usize = 0;
pub const RECORD_EPHEMERAL_KEY_OFFSET: usize = 32;
pub const RECORD_ENC_CIPHERTEXT_OFFSET: usize = 64;
pub const RECORD_CV_NET_OFFSET: usize = 644;
pub const RECORD_OUT_CIPHERTEXT_OFFSET: usize = 676;
pub const RECORD_TXID_OFFSET: usize = 756;
pub const RECORD_HEIGHT_OFFSET: usize = 788;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionRecord(pub [u8; RECORD_BYTES]);

pub struct ActionRecordParts {
    pub nullifier: [u8; 32],
    pub ephemeral_key: [u8; 32],
    pub enc_ciphertext: [u8; 580],
    pub cv_net: [u8; 32],
    pub out_ciphertext: [u8; 80],
    pub txid: [u8; 32],
    pub height: u32,
}

impl ActionRecord {
    pub fn from_parts(parts: ActionRecordParts) -> Self {
        let mut bytes = [0; RECORD_BYTES];
        bytes[RECORD_NULLIFIER_OFFSET..RECORD_EPHEMERAL_KEY_OFFSET]
            .copy_from_slice(&parts.nullifier);
        bytes[RECORD_EPHEMERAL_KEY_OFFSET..RECORD_ENC_CIPHERTEXT_OFFSET]
            .copy_from_slice(&parts.ephemeral_key);
        bytes[RECORD_ENC_CIPHERTEXT_OFFSET..RECORD_CV_NET_OFFSET]
            .copy_from_slice(&parts.enc_ciphertext);
        bytes[RECORD_CV_NET_OFFSET..RECORD_OUT_CIPHERTEXT_OFFSET].copy_from_slice(&parts.cv_net);
        bytes[RECORD_OUT_CIPHERTEXT_OFFSET..RECORD_TXID_OFFSET]
            .copy_from_slice(&parts.out_ciphertext);
        bytes[RECORD_TXID_OFFSET..RECORD_HEIGHT_OFFSET].copy_from_slice(&parts.txid);
        bytes[RECORD_HEIGHT_OFFSET..].copy_from_slice(&parts.height.to_le_bytes());
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8; RECORD_BYTES] {
        &self.0
    }

    pub fn nullifier(&self) -> &[u8; 32] {
        self.0[RECORD_NULLIFIER_OFFSET..RECORD_EPHEMERAL_KEY_OFFSET]
            .try_into()
            .expect("fixed slice")
    }

    pub fn ephemeral_key(&self) -> &[u8; 32] {
        self.0[RECORD_EPHEMERAL_KEY_OFFSET..RECORD_ENC_CIPHERTEXT_OFFSET]
            .try_into()
            .expect("fixed slice")
    }

    pub fn enc_ciphertext(&self) -> &[u8; 580] {
        self.0[RECORD_ENC_CIPHERTEXT_OFFSET..RECORD_CV_NET_OFFSET]
            .try_into()
            .expect("fixed slice")
    }

    pub fn cv_net(&self) -> &[u8; 32] {
        self.0[RECORD_CV_NET_OFFSET..RECORD_OUT_CIPHERTEXT_OFFSET]
            .try_into()
            .expect("fixed slice")
    }

    pub fn out_ciphertext(&self) -> &[u8; 80] {
        self.0[RECORD_OUT_CIPHERTEXT_OFFSET..RECORD_TXID_OFFSET]
            .try_into()
            .expect("fixed slice")
    }

    pub fn txid(&self) -> &[u8; 32] {
        self.0[RECORD_TXID_OFFSET..RECORD_HEIGHT_OFFSET]
            .try_into()
            .expect("fixed slice")
    }

    pub fn height(&self) -> u32 {
        u32::from_le_bytes(
            self.0[RECORD_HEIGHT_OFFSET..]
                .try_into()
                .expect("fixed slice"),
        )
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
    ACTION_LAYOUT.logical_rows_for(used_rows)
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
        assert_eq!(RECORD_BYTES, 792);
        assert_eq!(ROW_BYTES, 6_336);
        assert_eq!(RECORD_HEIGHT_OFFSET + 4, RECORD_BYTES);
        assert_eq!(SHARD_POSITIONS, 65_536);
        assert_eq!(SHARD_ROWS % 2_048, 0);
        assert_eq!(logical_rows_for(0), 8_192);
        assert_eq!(logical_rows_for(16_819), 32_768);
    }

    #[test]
    fn record_layout_is_fixed_and_round_trips() {
        let record = ActionRecord::from_parts(ActionRecordParts {
            nullifier: [1; 32],
            ephemeral_key: [2; 32],
            enc_ciphertext: [3; 580],
            cv_net: [4; 32],
            out_ciphertext: [5; 80],
            txid: [6; 32],
            height: 0x0403_0201,
        });
        let bytes = record.as_bytes();
        assert_eq!(&bytes[..32], &[1; 32]);
        assert_eq!(&bytes[32..64], &[2; 32]);
        assert_eq!(&bytes[64..644], &[3; 580][..]);
        assert_eq!(&bytes[644..676], &[4; 32]);
        assert_eq!(&bytes[676..756], &[5; 80][..]);
        assert_eq!(&bytes[756..788], &[6; 32]);
        assert_eq!(&bytes[788..], &[1, 2, 3, 4]);
        assert_eq!(record.nullifier(), &[1; 32]);
        assert_eq!(record.ephemeral_key(), &[2; 32]);
        assert_eq!(record.enc_ciphertext(), &[3; 580]);
        assert_eq!(record.cv_net(), &[4; 32]);
        assert_eq!(record.out_ciphertext(), &[5; 80]);
        assert_eq!(record.txid(), &[6; 32]);
        assert_eq!(record.height(), 0x0403_0201);
    }

    #[test]
    fn layout_derivations_match_the_action_constants() {
        assert_eq!(ACTION_LAYOUT.row_bytes(), 6_336);
        assert_eq!(ACTION_LAYOUT.shard_positions(), 65_536);
        assert_eq!(ACTION_LAYOUT.item_size_bits(), 50_688);
        assert_eq!(ACTION_LAYOUT.shard_bytes(), 51_904_512);
        assert_eq!(ACTION_LAYOUT.used_rows_for(0), 0);
        assert_eq!(ACTION_LAYOUT.used_rows_for(9), 2);
        assert_eq!(ACTION_LAYOUT.row_for_position(65_537), (8_192, 1));
        assert!(ACTION_LAYOUT.shard_rows.is_multiple_of(2_048));
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
