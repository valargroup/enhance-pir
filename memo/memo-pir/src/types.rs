use serde::{Deserialize, Serialize};

pub const SCHEMA_VERSION: u16 = 3;
pub const NETWORK: &str = "main";
pub const POOL: &str = "ironwood";
pub const ACTIVATION_HEIGHT: u64 = 3_428_143;
pub const CONFIRMATIONS: u64 = 10;
/// One Ironwood action record. Layout, in order:
///
/// ```text
/// nf[32] ‖ ephemeralKey[32] ‖ encCiphertext[580] ‖ cmx[32] ‖ cv_net[32] ‖ outCiphertext[80] ‖ txid[32] ‖ height[4 LE]
/// ```
///
/// `nf` is the action's spent nullifier, which is `rho` of the output note and
/// is required to trial-decrypt an unknown note. `cv_net` and `outCiphertext`
/// allow outgoing recovery under the OVK. `txid` is in internal (little-endian)
/// byte order. Everything a wallet needs to reconstruct the action except the
/// proof and signature is here; `cm_x` is recomputed from the decrypted note.
pub use pir_types::{
    seed_from_domain, setup_seed_bytes, DatabaseId, DatabaseLayout, Envelope, GenerationManifest,
    ShardDescriptor, TableManifest, ACTION_LAYOUT, COLD_CHECKPOINT_INTERVAL, DEFAULT_ENVELOPE,
    MANIFEST_SCHEMA_VERSION, MEMO_SETUP_SEED, NULLIFIER_LAYOUT, PROTOCOL_REVISION, WITNESS_LAYOUT,
};

// Projections of `ACTION_LAYOUT` kept for the ACTION-specific code paths
// (record parsing) that are not layout-parameterized.
pub const RECORD_BYTES: usize = ACTION_LAYOUT.record_bytes;
pub const RECORDS_PER_ROW: usize = ACTION_LAYOUT.records_per_row;
pub const ROW_BYTES: usize = ACTION_LAYOUT.row_bytes();
pub const SHARD_ROWS: usize = ACTION_LAYOUT.shard_rows;
pub const SHARD_POSITIONS: usize = ACTION_LAYOUT.shard_positions();
/// Fixed placement quantum. Worker `n` owns shard IDs `n * 2..(n + 1) * 2`.
/// Appending workers therefore never moves an already-published shard.
pub const SHARDS_PER_WORKER: u64 = 2;
pub const ITEM_SIZE_BITS: u64 = ACTION_LAYOUT.item_size_bits();

pub const RECORD_NULLIFIER_OFFSET: usize = 0;
pub const RECORD_EPHEMERAL_KEY_OFFSET: usize = 32;
pub const RECORD_ENC_CIPHERTEXT_OFFSET: usize = 64;
pub const RECORD_CMX_OFFSET: usize = 644;
pub const RECORD_CV_NET_OFFSET: usize = 676;
pub const RECORD_OUT_CIPHERTEXT_OFFSET: usize = 708;
pub const RECORD_TXID_OFFSET: usize = 788;
pub const RECORD_HEIGHT_OFFSET: usize = 820;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActionRecord(pub [u8; RECORD_BYTES]);

impl AsRef<[u8]> for ActionRecord {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

pub struct ActionRecordParts {
    pub nullifier: [u8; 32],
    pub ephemeral_key: [u8; 32],
    pub enc_ciphertext: [u8; 580],
    pub cmx: [u8; 32],
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
        bytes[RECORD_ENC_CIPHERTEXT_OFFSET..RECORD_CMX_OFFSET]
            .copy_from_slice(&parts.enc_ciphertext);
        bytes[RECORD_CMX_OFFSET..RECORD_CV_NET_OFFSET].copy_from_slice(&parts.cmx);
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
        self.0[RECORD_ENC_CIPHERTEXT_OFFSET..RECORD_CMX_OFFSET]
            .try_into()
            .expect("fixed slice")
    }

    /// The output note's extracted commitment, so a trial-decrypted note can
    /// be authenticated against the record itself.
    pub fn cmx(&self) -> &[u8; 32] {
        self.0[RECORD_CMX_OFFSET..RECORD_CV_NET_OFFSET]
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

/// Positions a snapshot represents. Only full coverage from position zero is
/// served; the tagged shape is kept because clients pin it on the wire.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum Coverage {
    Full { covered_position_start: u64 },
}

impl Coverage {
    pub const fn full() -> Self {
        Self::Full {
            covered_position_start: 0,
        }
    }

    pub fn covered_position_start(&self) -> u64 {
        match self {
            Self::Full {
                covered_position_start,
            } => *covered_position_start,
        }
    }
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
    /// The legacy single-table view of a generation, served on `/memo/metadata`
    /// until every client reads the manifest.
    pub fn from_manifest(manifest: &GenerationManifest) -> Option<Self> {
        let table = manifest.tables.get(&DatabaseId::Action)?;
        Some(Self {
            schema_version: SCHEMA_VERSION,
            network: manifest.network.clone(),
            pool: manifest.pool.clone(),
            anchor_height: manifest.anchor_height,
            anchor_block_hash: manifest.anchor_block_hash.clone(),
            ironwood_tree_size: manifest.ironwood_tree_size,
            coverage: Coverage::full(),
            record_bytes: table.record_bytes,
            records_per_row: table.records_per_row,
            row_bytes: table.row_bytes,
            shard_rows: table.shard_rows,
            used_rows: table.used_rows,
            logical_rows: table.logical_rows,
            first_global_row: 0,
            generation: manifest.generation,
            parameter_id: table.parameter_id.clone(),
            setup_seed: table.setup_seed,
            public_params_epoch: table.public_params_epoch.clone(),
            public_params_sha256: table.public_params_sha256.clone(),
            shards: table.shards.clone(),
        })
    }

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

/// Which worker of an ordered pool owns a shard. A single-worker pool owns
/// everything (development); otherwise ownership is a pure function of the
/// shard id so appending workers never moves a published shard.
pub fn worker_index_for_shard<T>(shard_id: u64, pool: &[T]) -> Option<usize> {
    match pool.len() {
        0 => None,
        1 => Some(0),
        count => {
            let index = usize::try_from(shard_id / SHARDS_PER_WORKER).ok()?;
            (index < count).then_some(index)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn geometry_is_fixed_and_aligned() {
        assert_eq!(RECORD_BYTES, 824);
        assert_eq!(ROW_BYTES, 6_592);
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
            cmx: [9; 32],
            cv_net: [4; 32],
            out_ciphertext: [5; 80],
            txid: [6; 32],
            height: 0x0403_0201,
        });
        let bytes = record.as_bytes();
        assert_eq!(&bytes[..32], &[1; 32]);
        assert_eq!(&bytes[32..64], &[2; 32]);
        assert_eq!(&bytes[64..644], &[3; 580][..]);
        assert_eq!(&bytes[644..676], &[9; 32]);
        assert_eq!(&bytes[676..708], &[4; 32]);
        assert_eq!(&bytes[708..788], &[5; 80][..]);
        assert_eq!(&bytes[788..820], &[6; 32]);
        assert_eq!(&bytes[820..], &[1, 2, 3, 4]);
        assert_eq!(record.nullifier(), &[1; 32]);
        assert_eq!(record.ephemeral_key(), &[2; 32]);
        assert_eq!(record.enc_ciphertext(), &[3; 580]);
        assert_eq!(record.cmx(), &[9; 32]);
        assert_eq!(record.cv_net(), &[4; 32]);
        assert_eq!(record.out_ciphertext(), &[5; 80]);
        assert_eq!(record.txid(), &[6; 32]);
        assert_eq!(record.height(), 0x0403_0201);
    }

    #[test]
    fn adding_workers_does_not_move_sealed_shards() {
        let two = ["a", "b"];
        let three = ["a", "b", "c"];
        for shard in 0..4 {
            assert_eq!(
                worker_index_for_shard(shard, &two),
                Some((shard / 2) as usize)
            );
            assert_eq!(
                worker_index_for_shard(shard, &three),
                Some((shard / 2) as usize)
            );
        }
        assert_eq!(worker_index_for_shard(4, &two), None);
        assert_eq!(worker_index_for_shard(4, &three), Some(2));
        assert_eq!(worker_index_for_shard(9, &["solo"]), Some(0));
        assert_eq!(worker_index_for_shard::<&str>(0, &[]), None);
    }
}
