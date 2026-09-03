//! Derives every table's journal from one finalized block.
//!
//! One Zakura fetch feeds four append-only journals under `data_dir/{name}`:
//!
//! | journal          | record                                   | positions          |
//! |------------------|------------------------------------------|--------------------|
//! | `action`         | 824-byte action record                   | commitment position|
//! | `witness`        | 32-byte `cmx`                            | commitment position|
//! | `witness-roots`  | 32-byte root of a *completed* sub-shard  | sub-shard index    |
//! | `nullifiers`     | 41-byte `nf ‖ height ‖ first_pos ‖ count` | spend log order    |
//!
//! Journals never mutate, so sealed-shard logic applies to each; the frontier
//! sub-shard's root is public chain data and is served in the broadcast cap
//! rather than journaled.

use crate::store::{RecordJournal, StoreError};
use crate::types::{DatabaseId, DatabaseLayout, ACTION_LAYOUT, WITNESS_LAYOUT};
use crate::zakura::CanonicalBlock;
use std::path::{Path, PathBuf};

/// Leaves per sub-shard (tree levels 0 to 8).
pub const SUBSHARD_LEAVES: u64 = 256;

/// Bytes of one nullifier journal entry: `nf[32] ‖ spend_height u32 ‖
/// first_output_position u32 ‖ action_count u8`, the nullifier-PIR bucket
/// entry format, so bucket tables are built by copying entries.
pub const NULLIFIER_ENTRY_BYTES: usize = 41;

/// Layout the nullifier log is journaled under. It is never served as a
/// table itself; the cold and warm bucket tables are built from it.
pub const NULLIFIER_LOG_LAYOUT: DatabaseLayout = DatabaseLayout {
    record_bytes: NULLIFIER_ENTRY_BYTES,
    records_per_row: 1,
    shard_rows: 8_192,
};

pub const NULLIFIER_LOG_NAME: &str = "nullifiers";

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("journal error: {0}")]
    Store(#[from] StoreError),
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("ingest invariant violated: {0}")]
    Invariant(String),
}

/// One nullifier as logged: the spending transaction's place in the tree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct NullifierEntry {
    pub nullifier: [u8; 32],
    pub spend_height: u32,
    pub first_output_position: u32,
    pub action_count: u8,
}

impl NullifierEntry {
    pub fn to_bytes(&self) -> [u8; NULLIFIER_ENTRY_BYTES] {
        let mut bytes = [0u8; NULLIFIER_ENTRY_BYTES];
        bytes[..32].copy_from_slice(&self.nullifier);
        bytes[32..36].copy_from_slice(&self.spend_height.to_le_bytes());
        bytes[36..40].copy_from_slice(&self.first_output_position.to_le_bytes());
        bytes[40] = self.action_count;
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != NULLIFIER_ENTRY_BYTES {
            return None;
        }
        Some(Self {
            nullifier: bytes[..32].try_into().ok()?,
            spend_height: u32::from_le_bytes(bytes[32..36].try_into().ok()?),
            first_output_position: u32::from_le_bytes(bytes[36..40].try_into().ok()?),
            action_count: bytes[40],
        })
    }
}

/// Every journal the coordinator publishes from, kept in step block by block.
pub struct Journals {
    pub action: RecordJournal,
    pub witness: RecordJournal,
    pub witness_roots: RecordJournal,
    pub nullifiers: RecordJournal,
}

impl Journals {
    /// Opens (or creates) the journals under `data_dir/{name}`. A journal
    /// left at the flat legacy location (`data_dir/records.bin`) is moved
    /// under `action/` first so the version check sets it aside cleanly.
    pub fn open(data_dir: &Path) -> Result<Self, IngestError> {
        migrate_flat_action_journal(data_dir)?;
        Ok(Self {
            action: RecordJournal::open(
                data_dir.join(DatabaseId::Action.as_str()),
                DatabaseId::Action,
                ACTION_LAYOUT,
            )?,
            witness: RecordJournal::open(
                data_dir.join(DatabaseId::Witness.as_str()),
                DatabaseId::Witness,
                WITNESS_LAYOUT,
            )?,
            witness_roots: RecordJournal::open(
                data_dir.join(DatabaseId::WitnessRoots.as_str()),
                DatabaseId::WitnessRoots,
                WITNESS_LAYOUT,
            )?,
            nullifiers: RecordJournal::open_log(
                data_dir.join(NULLIFIER_LOG_NAME),
                NULLIFIER_LOG_NAME,
                NULLIFIER_LOG_LAYOUT,
            )?,
        })
    }

    fn all(&self) -> [&RecordJournal; 4] {
        [
            &self.action,
            &self.witness,
            &self.witness_roots,
            &self.nullifiers,
        ]
    }

    /// The lowest committed height across journals, or `None` if any journal
    /// is empty. Ingest resumes from the block after it.
    pub fn committed_height(&self) -> Option<u64> {
        self.all()
            .iter()
            .map(|journal| journal.last_block().map(|block| block.height))
            .min()
            .flatten()
    }

    /// The highest committed height across journals, for the reorg check.
    pub fn highest_committed(&self) -> Option<(u64, String)> {
        self.all()
            .iter()
            .filter_map(|journal| journal.last_block())
            .max_by_key(|block| block.height)
            .map(|block| (block.height, block.hash.clone()))
    }

    /// Whether every journal has committed `height`.
    pub fn all_at(&self, height: u64) -> bool {
        self.all().iter().all(|journal| {
            journal
                .last_block()
                .is_some_and(|block| block.height == height)
        })
    }

    /// Appends one block to every journal that has not committed it yet.
    /// Journals may be at different heights after a crash between appends;
    /// each catches up independently and the block is skipped where present.
    pub fn append_block(&mut self, block: &CanonicalBlock) -> Result<(), IngestError> {
        let first_position = block
            .first_position()
            .ok_or_else(|| IngestError::Invariant("block action count exceeds tree size".into()))?;
        let height32 = u32::try_from(block.height)
            .map_err(|_| IngestError::Invariant(format!("height {} exceeds u32", block.height)))?;

        if needs(&self.action, block.height) {
            check_continuity(&self.action, first_position, block.tree_size, block.height)?;
            let skip = (self.action.tree_size() - first_position) as usize;
            self.action
                .append_block(block.height, block.hash.clone(), &block.records[skip..])?;
        }

        if needs(&self.witness, block.height) {
            check_continuity(&self.witness, first_position, block.tree_size, block.height)?;
            let skip = (self.witness.tree_size() - first_position) as usize;
            let cmxs: Vec<[u8; 32]> = block
                .transactions
                .iter()
                .flat_map(|tx| tx.cmxs.iter().copied())
                .collect();
            self.witness
                .append_block(block.height, block.hash.clone(), &cmxs[skip..])?;
        }

        if needs(&self.witness_roots, block.height) {
            // Sub-shards completed by this block: those whose last leaf index
            // is below the new tree size and at or above the old one.
            let completed_before = self.witness_roots.tree_size();
            let completed_after = block.tree_size / SUBSHARD_LEAVES;
            if completed_before > completed_after {
                return Err(IngestError::Invariant(format!(
                    "witness-roots holds {completed_before} sub-shards but the tree has {completed_after}"
                )));
            }
            let mut roots = Vec::new();
            for subshard in completed_before..completed_after {
                let leaves = self
                    .witness
                    .read_records(subshard * SUBSHARD_LEAVES, SUBSHARD_LEAVES as usize)?;
                let hashes: Vec<[u8; 32]> = leaves
                    .chunks_exact(32)
                    .map(|chunk| chunk.try_into().expect("32-byte leaf"))
                    .collect();
                roots.push(commitment_tree_db::complete_subtree_root(&hashes, 0));
            }
            self.witness_roots
                .append_block(block.height, block.hash.clone(), &roots)?;
        }

        if needs(&self.nullifiers, block.height) {
            let mut entries = Vec::new();
            for tx in &block.transactions {
                let first_output_position =
                    u32::try_from(first_position + tx.first_action_index as u64).map_err(|_| {
                        IngestError::Invariant("output position exceeds u32".to_string())
                    })?;
                let action_count = u8::try_from(tx.action_count()).map_err(|_| {
                    IngestError::Invariant("transaction has more than 255 actions".to_string())
                })?;
                for nullifier in &tx.nullifiers {
                    entries.push(
                        NullifierEntry {
                            nullifier: *nullifier,
                            spend_height: height32,
                            first_output_position,
                            action_count,
                        }
                        .to_bytes(),
                    );
                }
            }
            self.nullifiers
                .append_block(block.height, block.hash.clone(), &entries)?;
        }

        if self.action.tree_size() != block.tree_size || self.witness.tree_size() != block.tree_size
        {
            return Err(IngestError::Invariant(format!(
                "Ironwood tree size mismatch at height {}: action {}, witness {}, Zakura {}",
                block.height,
                self.action.tree_size(),
                self.witness.tree_size(),
                block.tree_size
            )));
        }
        Ok(())
    }
}

fn needs(journal: &RecordJournal, height: u64) -> bool {
    journal
        .last_block()
        .is_none_or(|block| block.height < height)
}

fn check_continuity(
    journal: &RecordJournal,
    first_position: u64,
    tree_size_after: u64,
    height: u64,
) -> Result<(), IngestError> {
    if journal.tree_size() < first_position || journal.tree_size() > tree_size_after {
        return Err(IngestError::Invariant(format!(
            "{} continuity mismatch at height {height}: journal {}, block {first_position}..{tree_size_after}",
            journal.name(),
            journal.tree_size()
        )));
    }
    Ok(())
}

/// Moves a pre-table ACTION journal from `data_dir/` to `data_dir/action/`.
fn migrate_flat_action_journal(data_dir: &Path) -> Result<(), IngestError> {
    let flat_records = data_dir.join("records.bin");
    let flat_manifest = data_dir.join("manifest.json");
    if !flat_records.exists() && !flat_manifest.exists() {
        return Ok(());
    }
    let target: PathBuf = data_dir.join(DatabaseId::Action.as_str());
    std::fs::create_dir_all(&target)?;
    for (from, name) in [
        (flat_records, "records.bin"),
        (flat_manifest, "manifest.json"),
    ] {
        if from.exists() {
            std::fs::rename(&from, target.join(name))?;
        }
    }
    tracing::info!(path = %target.display(), "moved the flat action journal under its table directory");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ActionRecord, ActionRecordParts};
    use crate::zakura::CanonicalTx;

    fn record(position: u64, cmx: [u8; 32], nullifier: [u8; 32]) -> ActionRecord {
        ActionRecord::from_parts(ActionRecordParts {
            nullifier,
            ephemeral_key: [2; 32],
            enc_ciphertext: [3; 580],
            cmx,
            cv_net: [4; 32],
            out_ciphertext: [5; 80],
            txid: [7; 32],
            height: 3_428_143 + position as u32,
        })
    }

    /// A block of `txs` transactions with `per_tx` actions each, starting at
    /// `first_position`. Leaves are distinct small values.
    fn block(height: u64, first_position: u64, txs: usize, per_tx: usize) -> CanonicalBlock {
        let mut records = Vec::new();
        let mut transactions = Vec::new();
        let mut position = first_position;
        for tx in 0..txs {
            let mut nullifiers = Vec::new();
            let mut cmxs = Vec::new();
            for _ in 0..per_tx {
                let mut cmx = [0u8; 32];
                cmx[..8].copy_from_slice(&position.to_le_bytes());
                let mut nf = [0xAAu8; 32];
                nf[..8].copy_from_slice(&position.to_le_bytes());
                records.push(record(position, cmx, nf));
                nullifiers.push(nf);
                cmxs.push(cmx);
                position += 1;
            }
            transactions.push(CanonicalTx {
                txid: [tx as u8; 32],
                first_action_index: tx * per_tx,
                nullifiers,
                cmxs,
            });
        }
        CanonicalBlock {
            height,
            hash: format!("{height:064x}"),
            records,
            transactions,
            tree_size: position,
        }
    }

    #[test]
    fn one_block_feeds_all_four_journals_with_the_right_positions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut journals = Journals::open(dir.path()).expect("open");
        assert_eq!(journals.committed_height(), None);

        // 3 txs × 100 actions = 300 positions: sub-shard 0 completes at 256.
        let first = block(3_428_143, 0, 3, 100);
        journals.append_block(&first).expect("append");
        assert!(journals.all_at(3_428_143));
        assert_eq!(journals.action.tree_size(), 300);
        assert_eq!(journals.witness.tree_size(), 300);
        assert_eq!(journals.witness_roots.tree_size(), 1);
        assert_eq!(journals.nullifiers.tree_size(), 300);

        // The completed sub-shard root is the root over its 256 leaves.
        let leaves = journals.witness.read_records(0, 256).expect("leaves");
        let hashes: Vec<[u8; 32]> = leaves
            .chunks_exact(32)
            .map(|c| c.try_into().unwrap())
            .collect();
        let expected = commitment_tree_db::complete_subtree_root(&hashes, 0);
        assert_eq!(journals.witness_roots.read_records(0, 1).unwrap(), expected);

        // Nullifier entries carry the spending transaction's first position.
        let entries = journals.nullifiers.read_records(0, 300).expect("entries");
        let second_tx = NullifierEntry::from_bytes(&entries[150 * 41..151 * 41]).unwrap();
        assert_eq!(second_tx.spend_height, 3_428_143);
        assert_eq!(second_tx.first_output_position, 100);
        assert_eq!(second_tx.action_count, 100);
        assert_eq!(&second_tx.nullifier[..8], &150u64.to_le_bytes());

        // A second block completes nothing new; a third crosses 512.
        journals
            .append_block(&block(3_428_144, 300, 1, 12))
            .expect("append");
        assert_eq!(journals.witness_roots.tree_size(), 1);
        journals
            .append_block(&block(3_428_145, 312, 2, 100))
            .expect("append");
        assert_eq!(journals.witness_roots.tree_size(), 2);
        assert_eq!(journals.committed_height(), Some(3_428_145));
    }

    #[test]
    fn journals_catch_up_independently_after_a_partial_append() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut journals = Journals::open(dir.path()).expect("open");
        let first = block(10, 0, 1, 5);
        // Simulate a crash after only the action journal committed the block.
        journals
            .action
            .append_block(10, first.hash.clone(), &first.records)
            .unwrap();
        assert_eq!(journals.committed_height(), None);
        journals.append_block(&first).expect("catch up");
        assert!(journals.all_at(10));
        assert_eq!(journals.action.tree_size(), 5);
        assert_eq!(journals.witness.tree_size(), 5);
    }

    #[test]
    fn a_flat_legacy_journal_is_moved_under_the_action_directory() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("manifest.json"),
            br#"{"version":2,"tree_size":1,"blocks":[{"height":5,"hash":"aa","first_position":0,"action_count":1}]}"#,
        )
        .unwrap();
        std::fs::write(dir.path().join("records.bin"), vec![1u8; 792]).unwrap();
        let journals = Journals::open(dir.path()).expect("open");
        assert!(!dir.path().join("records.bin").exists());
        assert!(dir.path().join("action/superseded-v2-action").exists());
        assert_eq!(journals.action.tree_size(), 0);
    }
}
