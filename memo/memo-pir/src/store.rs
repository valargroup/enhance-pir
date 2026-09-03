use crate::types::{DatabaseId, DatabaseLayout};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

// Version 2: 792-byte action records (schema version 2). A version-1 journal
// holds 612-byte memo records and must be re-ingested from activation.
const STORE_VERSION: u16 = 2;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("manifest error: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("store invariant violated: {0}")]
    Invariant(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct BlockEntry {
    pub height: u64,
    pub hash: String,
    pub first_position: u64,
    pub action_count: u64,
}

fn default_table() -> String {
    // Journals written before tables were named are ACTION journals.
    DatabaseId::Action.as_str().to_string()
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoreManifest {
    version: u16,
    #[serde(default = "default_table")]
    table: String,
    tree_size: u64,
    blocks: Vec<BlockEntry>,
}

/// Append-only journal of fixed-size records for one table, indexed by
/// commitment-tree position from zero. Position to offset is
/// `position * layout.record_bytes`.
pub struct RecordJournal {
    dir: PathBuf,
    records_path: PathBuf,
    table: DatabaseId,
    layout: DatabaseLayout,
    manifest: StoreManifest,
}

impl RecordJournal {
    pub fn open(
        path: impl AsRef<Path>,
        table: DatabaseId,
        layout: DatabaseLayout,
    ) -> Result<Self, StoreError> {
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let records_path = dir.join("records.bin");
        let manifest_path = dir.join("manifest.json");
        let fresh = || StoreManifest {
            version: STORE_VERSION,
            table: table.as_str().to_string(),
            tree_size: 0,
            blocks: Vec::new(),
        };
        let manifest = if manifest_path.exists() {
            let bytes = fs::read(&manifest_path)?;
            let parsed: StoreManifest = serde_json::from_slice(&bytes)?;
            if parsed.version != STORE_VERSION || parsed.table != table.as_str() {
                // An older or foreign journal holds a different record layout and
                // cannot be converted. Set it aside rather than refuse to start: the
                // archive node re-derives everything, and a restart must not need an
                // operator on the host.
                let superseded =
                    dir.join(format!("superseded-v{}-{}", parsed.version, parsed.table));
                fs::create_dir_all(&superseded)?;
                fs::rename(&records_path, superseded.join("records.bin"))?;
                fs::rename(&manifest_path, superseded.join("manifest.json"))?;
                File::open(&dir)?.sync_all()?;
                tracing::warn!(
                    found = parsed.version,
                    found_table = %parsed.table,
                    expected = STORE_VERSION,
                    table = %table,
                    path = %superseded.display(),
                    "set aside an incompatible journal; re-ingesting from activation"
                );
                fresh()
            } else {
                parsed
            }
        } else {
            fresh()
        };

        let expected_len = manifest
            .tree_size
            .checked_mul(layout.record_bytes as u64)
            .ok_or_else(|| StoreError::Invariant("record length overflow".to_string()))?;
        let records = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&records_path)?;
        if records.metadata()?.len() < expected_len {
            return Err(StoreError::Invariant(
                "records file is shorter than committed manifest".to_string(),
            ));
        }
        records.set_len(expected_len)?;

        let store = Self {
            dir,
            records_path,
            table,
            layout,
            manifest,
        };
        if !store.manifest_path().exists() {
            store.persist_manifest()?;
        }
        Ok(store)
    }

    pub fn table(&self) -> DatabaseId {
        self.table
    }

    pub fn layout(&self) -> &DatabaseLayout {
        &self.layout
    }

    pub fn tree_size(&self) -> u64 {
        self.manifest.tree_size
    }

    pub fn last_block(&self) -> Option<&BlockEntry> {
        self.manifest.blocks.last()
    }

    pub fn blocks(&self) -> &[BlockEntry] {
        &self.manifest.blocks
    }

    pub fn append_block<R: AsRef<[u8]>>(
        &mut self,
        height: u64,
        hash: String,
        records: &[R],
    ) -> Result<(), StoreError> {
        if let Some(previous) = self.last_block() {
            if height != previous.height + 1 {
                return Err(StoreError::Invariant(format!(
                    "block height {height} does not follow {}",
                    previous.height
                )));
            }
        }
        if let Some(bad) = records
            .iter()
            .find(|record| record.as_ref().len() != self.layout.record_bytes)
        {
            return Err(StoreError::Invariant(format!(
                "record has {} bytes, layout needs {}",
                bad.as_ref().len(),
                self.layout.record_bytes
            )));
        }

        let first_position = self.manifest.tree_size;
        let mut file = OpenOptions::new().append(true).open(&self.records_path)?;
        for record in records {
            file.write_all(record.as_ref())?;
        }
        file.sync_all()?;

        self.manifest.tree_size = self
            .manifest
            .tree_size
            .checked_add(records.len() as u64)
            .ok_or_else(|| StoreError::Invariant("tree size overflow".to_string()))?;
        self.manifest.blocks.push(BlockEntry {
            height,
            hash,
            first_position,
            action_count: records.len() as u64,
        });
        self.persist_manifest()
    }

    /// Shards with at least one populated position, from shard zero.
    pub fn shard_ids(&self) -> std::ops::RangeInclusive<u64> {
        let last_position = self.manifest.tree_size.saturating_sub(1);
        0..=last_position / self.layout.shard_positions() as u64
    }

    /// The full padded shard: populated records in order, then zero bytes up
    /// to `layout.shard_bytes()`. Deterministic, so its digest is stable.
    pub fn read_shard_rows(&self, shard_id: u64) -> Result<Vec<u8>, StoreError> {
        let shard_positions = self.layout.shard_positions() as u64;
        let shard_start = shard_id
            .checked_mul(shard_positions)
            .ok_or_else(|| StoreError::Invariant("shard position overflow".to_string()))?;
        if shard_start >= self.manifest.tree_size {
            return Err(StoreError::Invariant(format!(
                "shard {shard_id} is outside stored coverage"
            )));
        }
        let available_positions = self
            .manifest
            .tree_size
            .saturating_sub(shard_start)
            .min(shard_positions) as usize;
        let record_bytes = self.layout.record_bytes;
        let mut rows = vec![0u8; self.layout.shard_bytes()];
        let mut file = File::open(&self.records_path)?;
        file.seek(SeekFrom::Start(shard_start * record_bytes as u64))?;
        file.read_exact(&mut rows[..available_positions * record_bytes])?;
        Ok(rows)
    }

    pub fn populated_positions_in_shard(&self, shard_id: u64) -> u64 {
        let shard_positions = self.layout.shard_positions() as u64;
        let start = shard_id.saturating_mul(shard_positions);
        self.manifest
            .tree_size
            .saturating_sub(start)
            .min(shard_positions)
    }

    pub fn rows_digest(rows: &[u8]) -> String {
        hex::encode(Sha256::digest(rows))
    }

    fn manifest_path(&self) -> PathBuf {
        self.dir.join("manifest.json")
    }

    fn persist_manifest(&self) -> Result<(), StoreError> {
        let path = self.manifest_path();
        let temporary = self.dir.join("manifest.json.tmp");
        let bytes = serde_json::to_vec_pretty(&self.manifest)?;
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        fs::rename(temporary, path)?;
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ActionRecord, ACTION_LAYOUT, RECORD_BYTES};

    fn record(byte: u8) -> ActionRecord {
        ActionRecord([byte; RECORD_BYTES])
    }

    fn open(dir: &Path) -> RecordJournal {
        RecordJournal::open(dir, DatabaseId::Action, ACTION_LAYOUT).expect("open")
    }

    #[test]
    fn append_restart_and_padding_are_deterministic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = open(dir.path());
        store
            .append_block(10, "aa".to_string(), &[record(1), record(2)])
            .expect("append");
        drop(store);

        let store = open(dir.path());
        assert_eq!(store.tree_size(), 2);
        assert_eq!(store.shard_ids(), 0..=0);
        let shard = store.read_shard_rows(0).expect("shard");
        assert_eq!(shard.len(), ACTION_LAYOUT.shard_bytes());
        assert_eq!(&shard[..RECORD_BYTES], &[1; RECORD_BYTES]);
        assert_eq!(&shard[RECORD_BYTES..2 * RECORD_BYTES], &[2; RECORD_BYTES]);
        assert!(shard[2 * RECORD_BYTES..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn records_must_match_the_layout() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = open(dir.path());
        let short = vec![0u8; RECORD_BYTES - 1];
        assert!(store.append_block(10, "aa".to_string(), &[short]).is_err());
        assert_eq!(store.tree_size(), 0);
    }

    #[test]
    fn older_or_foreign_journals_are_set_aside_and_restarted_empty() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("manifest.json"),
            br#"{"version":1,"base_position":0,"tree_size":2,"blocks":[{"height":5,"hash":"aa","first_position":0,"action_count":2}]}"#,
        )
        .expect("write manifest");
        std::fs::write(dir.path().join("records.bin"), vec![7u8; 2 * 612]).expect("write records");

        let store = open(dir.path());
        assert_eq!(store.tree_size(), 0);
        assert!(store.last_block().is_none());
        assert_eq!(
            std::fs::read(dir.path().join("superseded-v1-action/records.bin")).expect("kept"),
            vec![7u8; 2 * 612]
        );
        let manifest: serde_json::Value =
            serde_json::from_slice(&std::fs::read(dir.path().join("manifest.json")).expect("new"))
                .expect("json");
        assert_eq!(manifest["version"], 2);
        assert_eq!(manifest["table"], "action");

        // A current-version journal of another table is also foreign.
        drop(store);
        let foreign = RecordJournal::open(dir.path(), DatabaseId::Witness, ACTION_LAYOUT)
            .expect("open as another table");
        assert_eq!(foreign.tree_size(), 0);
        assert!(dir.path().join("superseded-v2-action").exists());
    }

    #[test]
    fn journals_without_a_table_name_are_action_journals() {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("manifest.json"),
            br#"{"version":2,"base_position":0,"tree_size":1,"blocks":[{"height":5,"hash":"aa","first_position":0,"action_count":1}]}"#,
        )
        .expect("write manifest");
        std::fs::write(dir.path().join("records.bin"), vec![9u8; RECORD_BYTES]).expect("records");
        let store = open(dir.path());
        assert_eq!(store.tree_size(), 1);
        assert_eq!(store.last_block().map(|block| block.height), Some(5));
    }
}
