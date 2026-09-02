use crate::types::{MemoRecord, RECORD_BYTES, ROW_BYTES, SHARD_POSITIONS, SHARD_ROWS};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const STORE_VERSION: u16 = 1;

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

#[derive(Clone, Debug, Serialize, Deserialize)]
struct StoreManifest {
    version: u16,
    base_position: u64,
    tree_size: u64,
    blocks: Vec<BlockEntry>,
}

pub struct MemoStore {
    dir: PathBuf,
    records_path: PathBuf,
    manifest: StoreManifest,
}

impl MemoStore {
    pub fn open(path: impl AsRef<Path>, base_position: u64) -> Result<Self, StoreError> {
        if !base_position.is_multiple_of(SHARD_POSITIONS as u64) {
            return Err(StoreError::Invariant(format!(
                "base position {base_position} is not shard aligned"
            )));
        }
        let dir = path.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let records_path = dir.join("records.bin");
        let manifest_path = dir.join("manifest.json");
        let manifest = if manifest_path.exists() {
            let bytes = fs::read(&manifest_path)?;
            let parsed: StoreManifest = serde_json::from_slice(&bytes)?;
            if parsed.version != STORE_VERSION
                || !parsed.base_position.is_multiple_of(SHARD_POSITIONS as u64)
            {
                return Err(StoreError::Invariant(
                    "store version or base position is invalid".to_string(),
                ));
            }
            parsed
        } else {
            StoreManifest {
                version: STORE_VERSION,
                base_position,
                tree_size: base_position,
                blocks: Vec::new(),
            }
        };

        let expected_len = manifest
            .tree_size
            .checked_sub(manifest.base_position)
            .and_then(|positions| positions.checked_mul(RECORD_BYTES as u64))
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
            manifest,
        };
        if !store.manifest_path().exists() {
            store.persist_manifest()?;
        }
        Ok(store)
    }

    pub fn base_position(&self) -> u64 {
        self.manifest.base_position
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

    pub fn append_block(
        &mut self,
        height: u64,
        hash: String,
        records: &[MemoRecord],
    ) -> Result<(), StoreError> {
        if let Some(previous) = self.last_block() {
            if height != previous.height + 1 {
                return Err(StoreError::Invariant(format!(
                    "block height {height} does not follow {}",
                    previous.height
                )));
            }
        }

        let first_position = self.manifest.tree_size;
        let mut file = OpenOptions::new().append(true).open(&self.records_path)?;
        for record in records {
            file.write_all(record.as_bytes())?;
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

    pub fn shard_ids(&self) -> std::ops::RangeInclusive<u64> {
        let first = self.manifest.base_position / SHARD_POSITIONS as u64;
        let last_position = self.manifest.tree_size.saturating_sub(1);
        let last = last_position.max(self.manifest.base_position) / SHARD_POSITIONS as u64;
        first..=last
    }

    pub fn read_shard_rows(&self, shard_id: u64) -> Result<Vec<u8>, StoreError> {
        let shard_start = shard_id
            .checked_mul(SHARD_POSITIONS as u64)
            .ok_or_else(|| StoreError::Invariant("shard position overflow".to_string()))?;
        if shard_start < self.manifest.base_position || shard_start >= self.manifest.tree_size {
            return Err(StoreError::Invariant(format!(
                "shard {shard_id} is outside stored coverage"
            )));
        }
        let available_positions = self
            .manifest
            .tree_size
            .saturating_sub(shard_start)
            .min(SHARD_POSITIONS as u64) as usize;
        let mut rows = vec![0u8; SHARD_ROWS * ROW_BYTES];
        let mut file = File::open(&self.records_path)?;
        let source_position = shard_start - self.manifest.base_position;
        file.seek(SeekFrom::Start(source_position * RECORD_BYTES as u64))?;
        file.read_exact(&mut rows[..available_positions * RECORD_BYTES])?;
        Ok(rows)
    }

    pub fn populated_positions_in_shard(&self, shard_id: u64) -> u64 {
        let start = shard_id.saturating_mul(SHARD_POSITIONS as u64);
        self.manifest
            .tree_size
            .saturating_sub(start)
            .min(SHARD_POSITIONS as u64)
    }

    pub fn effective_height_for_position(&self, position: u64) -> Option<u64> {
        self.manifest
            .blocks
            .iter()
            .find(|block| position < block.first_position + block.action_count)
            .map(|block| block.height)
            .or_else(|| self.manifest.blocks.first().map(|block| block.height))
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

    fn record(byte: u8) -> MemoRecord {
        MemoRecord([byte; RECORD_BYTES])
    }

    #[test]
    fn append_restart_and_padding_are_deterministic() {
        let dir = tempfile::tempdir().expect("tempdir");
        let mut store = MemoStore::open(dir.path(), 0).expect("open");
        store
            .append_block(10, "aa".to_string(), &[record(1), record(2)])
            .expect("append");
        drop(store);

        let store = MemoStore::open(dir.path(), 0).expect("reopen");
        assert_eq!(store.tree_size(), 2);
        let shard = store.read_shard_rows(0).expect("shard");
        assert_eq!(&shard[..RECORD_BYTES], &[1; RECORD_BYTES]);
        assert_eq!(&shard[RECORD_BYTES..2 * RECORD_BYTES], &[2; RECORD_BYTES]);
        assert!(shard[2 * RECORD_BYTES..].iter().all(|byte| *byte == 0));
    }

    #[test]
    fn base_position_must_be_shard_aligned() {
        let dir = tempfile::tempdir().expect("tempdir");
        assert!(MemoStore::open(dir.path(), 1).is_err());
    }
}
