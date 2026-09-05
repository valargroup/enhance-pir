//! Immutable per-block filter storage.
//!
//! Filter content is keyed by `(chain identity, profile, block hash)`, never by
//! height alone. Height is a mutable property of a branch; the filter for a
//! given block hash is not. That separation is what lets a reorg roll back
//! coverage without rewriting or re-deriving any filter bytes.
//!
//! On disk:
//!
//! - `meta.json`: chain identity, profile, start height, format version.
//! - `filters.bin`: append-only concatenated filter bytes.
//! - `index.bin`: one fixed 76-byte record per stored filter,
//!   `block_hash[32] | offset u64 | length u32 | digest[32]`.
//! - `chain.bin`: the replaceable height-to-hash map, `block_hash[32]` per
//!   covered height, ascending from `start_height`.
//! - `checkpoint.bin`: the durable commit point, holding the committed lengths
//!   of the other three files.
//!
//! A JSON manifest per block, as the Enhance journal uses, would rewrite a
//! growing document tens of thousands of times during backfill; the fixed-width
//! binary checkpoint follows `spend.rs` in the sibling server instead.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use transparent_filter::{filter_hash, BlockHash};

/// Bump when any on-disk layout changes. A mismatch sets the directory aside
/// and re-ingests rather than refusing to start.
const STORE_VERSION: u16 = 1;

const INDEX_RECORD_BYTES: usize = 32 + 8 + 4 + 32;
const CHAIN_RECORD_BYTES: usize = 32;

#[derive(Debug, thiserror::Error)]
pub enum StoreError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("metadata error: {0}")]
    Metadata(#[from] serde_json::Error),
    #[error("store invariant violated: {0}")]
    Invariant(String),
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
struct Meta {
    version: u16,
    profile: String,
    /// Chain identity, display hex, as a person would read it.
    genesis_hash: String,
    start_height: u64,
}

#[derive(Clone, Copy, Debug)]
struct Located {
    offset: u64,
    length: u32,
    digest: [u8; 32],
}

/// A stored filter with its provenance-free content identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StoredFilter {
    pub block_hash: BlockHash,
    pub bytes: Vec<u8>,
    pub digest: [u8; 32],
}

pub struct FilterStore {
    dir: PathBuf,
    meta: Meta,
    /// Block hash to location. Rebuilt from `index.bin` on open.
    by_hash: HashMap<BlockHash, Located>,
    /// Covered heights, ascending from `meta.start_height`.
    chain: Vec<BlockHash>,
    filters_len: u64,
}

fn read_u64(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes.try_into().expect("8 bytes"))
}

impl FilterStore {
    /// Opens or creates the store.
    ///
    /// An existing store whose version, profile, chain identity or start height
    /// differs is moved aside and a fresh one begun. The archive node can
    /// re-derive everything, and a restart must not require an operator on the
    /// host.
    pub fn open(
        dir: impl AsRef<Path>,
        profile: &str,
        genesis_hash: &str,
        start_height: u64,
    ) -> Result<Self, StoreError> {
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&dir)?;
        let wanted = Meta {
            version: STORE_VERSION,
            profile: profile.to_string(),
            genesis_hash: genesis_hash.to_string(),
            start_height,
        };
        let meta_path = dir.join("meta.json");
        if meta_path.exists() {
            let found: Result<Meta, _> = serde_json::from_slice(&std::fs::read(&meta_path)?);
            let compatible = matches!(&found, Ok(found) if *found == wanted);
            if !compatible {
                let label = match &found {
                    Ok(found) => format!("v{}-{}", found.version, found.profile),
                    Err(_) => "unreadable".to_string(),
                };
                let superseded = dir.join(format!("superseded-{label}"));
                std::fs::create_dir_all(&superseded)?;
                for name in [
                    "meta.json",
                    "filters.bin",
                    "index.bin",
                    "chain.bin",
                    "checkpoint.bin",
                ] {
                    let from = dir.join(name);
                    if from.exists() {
                        std::fs::rename(&from, superseded.join(name))?;
                    }
                }
                File::open(&dir)?.sync_all()?;
                tracing::warn!(
                    path = %superseded.display(),
                    "set aside an incompatible filter store; re-ingesting from the start height"
                );
            }
        }
        if !dir.join("meta.json").exists() {
            let bytes = serde_json::to_vec_pretty(&wanted)?;
            let temporary = dir.join("meta.json.tmp");
            let mut file = File::create(&temporary)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            std::fs::rename(temporary, dir.join("meta.json"))?;
            File::open(&dir)?.sync_all()?;
        }

        for name in ["filters.bin", "index.bin", "chain.bin"] {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(dir.join(name))?;
        }

        // Discard any tail written after the last durable checkpoint.
        let checkpoint_path = dir.join("checkpoint.bin");
        let (filters_len, index_len, chain_len) = if checkpoint_path.exists() {
            let bytes = std::fs::read(&checkpoint_path)?;
            if bytes.len() != 24 {
                return Err(StoreError::Invariant("checkpoint is not 24 bytes".into()));
            }
            (
                read_u64(&bytes[0..8]),
                read_u64(&bytes[8..16]),
                read_u64(&bytes[16..24]),
            )
        } else {
            (0, 0, 0)
        };
        for (name, length) in [
            ("filters.bin", filters_len),
            ("index.bin", index_len),
            ("chain.bin", chain_len),
        ] {
            let file = OpenOptions::new().write(true).open(dir.join(name))?;
            if file.metadata()?.len() < length {
                return Err(StoreError::Invariant(format!(
                    "{name} is shorter than its checkpoint"
                )));
            }
            file.set_len(length)?;
            file.sync_all()?;
        }

        let mut index_bytes = Vec::new();
        File::open(dir.join("index.bin"))?.read_to_end(&mut index_bytes)?;
        if index_bytes.len() % INDEX_RECORD_BYTES != 0 {
            return Err(StoreError::Invariant(
                "index.bin is not a whole number of records".into(),
            ));
        }
        let mut by_hash = HashMap::with_capacity(index_bytes.len() / INDEX_RECORD_BYTES);
        for record in index_bytes.chunks_exact(INDEX_RECORD_BYTES) {
            let hash = BlockHash::from_internal_bytes(record[0..32].try_into().expect("32"));
            by_hash.insert(
                hash,
                Located {
                    offset: read_u64(&record[32..40]),
                    length: u32::from_le_bytes(record[40..44].try_into().expect("4")),
                    digest: record[44..76].try_into().expect("32"),
                },
            );
        }

        let mut chain_bytes = Vec::new();
        File::open(dir.join("chain.bin"))?.read_to_end(&mut chain_bytes)?;
        if chain_bytes.len() % CHAIN_RECORD_BYTES != 0 {
            return Err(StoreError::Invariant(
                "chain.bin is not a whole number of records".into(),
            ));
        }
        let chain = chain_bytes
            .chunks_exact(CHAIN_RECORD_BYTES)
            .map(|record| BlockHash::from_internal_bytes(record.try_into().expect("32")))
            .collect();

        Ok(Self {
            dir,
            meta: wanted,
            by_hash,
            chain,
            filters_len,
        })
    }

    pub fn profile(&self) -> &str {
        &self.meta.profile
    }
    pub fn genesis_hash(&self) -> &str {
        &self.meta.genesis_hash
    }
    pub fn start_height(&self) -> u64 {
        self.meta.start_height
    }
    pub fn filters_stored(&self) -> u64 {
        self.by_hash.len() as u64
    }

    /// Highest height with durable coverage, if any.
    pub fn covered_through(&self) -> Option<u64> {
        if self.chain.is_empty() {
            None
        } else {
            Some(self.meta.start_height + self.chain.len() as u64 - 1)
        }
    }

    /// Height the next appended block must have.
    pub fn next_height(&self) -> u64 {
        self.covered_through()
            .map_or(self.meta.start_height, |height| height + 1)
    }

    /// Accepted block hash at `height` on the current branch.
    pub fn block_hash_at(&self, height: u64) -> Option<BlockHash> {
        let start = self.meta.start_height;
        if height < start {
            return None;
        }
        self.chain.get((height - start) as usize).copied()
    }

    pub fn height_of(&self, hash: BlockHash) -> Option<u64> {
        self.chain
            .iter()
            .position(|candidate| *candidate == hash)
            .map(|offset| self.meta.start_height + offset as u64)
    }

    /// Filter bytes for a block hash, whichever branch it is on.
    ///
    /// A filter for an orphaned block remains readable here. That is a cache of
    /// immutable content, not coverage: `block_hash_at` is what says whether a
    /// block is on the current branch.
    pub fn filter_by_hash(&self, hash: BlockHash) -> Result<Option<StoredFilter>, StoreError> {
        let Some(located) = self.by_hash.get(&hash).copied() else {
            return Ok(None);
        };
        let mut bytes = vec![0u8; located.length as usize];
        let mut file = File::open(self.dir.join("filters.bin"))?;
        file.seek(SeekFrom::Start(located.offset))?;
        file.read_exact(&mut bytes)?;
        Ok(Some(StoredFilter {
            block_hash: hash,
            bytes,
            digest: located.digest,
        }))
    }

    /// Appends one block's filter at the next height, extending coverage.
    ///
    /// Bytes are written before the checkpoint moves, so a crash loses the
    /// append rather than recording coverage for a filter that is not there.
    pub fn append(
        &mut self,
        height: u64,
        block_hash: BlockHash,
        filter: &[u8],
    ) -> Result<(), StoreError> {
        if height != self.next_height() {
            return Err(StoreError::Invariant(format!(
                "block at height {height} does not follow coverage through {:?}",
                self.covered_through()
            )));
        }
        let digest = filter_hash(filter).0;
        // The same block hash may already be stored, from a branch that was
        // rolled back and then re-accepted. Filter content is immutable, so an
        // existing entry is reused rather than rewritten; a differing digest
        // for the same block hash would mean the encoder is not deterministic.
        match self.by_hash.get(&block_hash) {
            Some(existing) if existing.digest != digest => {
                return Err(StoreError::Invariant(format!(
                    "block {} already has a filter with a different digest",
                    block_hash.to_display_hex()
                )));
            }
            Some(_) => {}
            None => {
                let offset = self.filters_len;
                let mut filters = OpenOptions::new()
                    .append(true)
                    .open(self.dir.join("filters.bin"))?;
                filters.write_all(filter)?;
                filters.sync_all()?;

                let mut record = Vec::with_capacity(INDEX_RECORD_BYTES);
                record.extend_from_slice(block_hash.internal_bytes());
                record.extend_from_slice(&offset.to_le_bytes());
                record.extend_from_slice(&(filter.len() as u32).to_le_bytes());
                record.extend_from_slice(&digest);
                let mut index = OpenOptions::new()
                    .append(true)
                    .open(self.dir.join("index.bin"))?;
                index.write_all(&record)?;
                index.sync_all()?;

                self.filters_len += filter.len() as u64;
                self.by_hash.insert(
                    block_hash,
                    Located {
                        offset,
                        length: filter.len() as u32,
                        digest,
                    },
                );
            }
        }

        let mut chain = OpenOptions::new()
            .append(true)
            .open(self.dir.join("chain.bin"))?;
        chain.write_all(block_hash.internal_bytes())?;
        chain.sync_all()?;
        self.chain.push(block_hash);
        Ok(())
    }

    /// Drops coverage above `height`, or all coverage when `None`.
    ///
    /// Filter bytes are kept. They are immutable content addressed by block
    /// hash, so a block that returns to the best chain needs no rebuild, and a
    /// block that does not simply stops being reachable through the height map.
    pub fn rollback_to(&mut self, height: Option<u64>) -> Result<(), StoreError> {
        let keep = match height {
            None => 0usize,
            Some(height) if height < self.meta.start_height => 0,
            Some(height) => ((height - self.meta.start_height + 1) as usize).min(self.chain.len()),
        };
        self.chain.truncate(keep);
        let file = OpenOptions::new()
            .write(true)
            .open(self.dir.join("chain.bin"))?;
        file.set_len((keep * CHAIN_RECORD_BYTES) as u64)?;
        file.sync_all()?;
        self.commit()
    }

    /// Makes everything written so far durable.
    pub fn commit(&mut self) -> Result<(), StoreError> {
        let mut bytes = Vec::with_capacity(24);
        bytes.extend_from_slice(&self.filters_len.to_le_bytes());
        bytes.extend_from_slice(&((self.by_hash.len() * INDEX_RECORD_BYTES) as u64).to_le_bytes());
        bytes.extend_from_slice(&((self.chain.len() * CHAIN_RECORD_BYTES) as u64).to_le_bytes());
        let temporary = self.dir.join("checkpoint.bin.tmp");
        let mut file = File::create(&temporary)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
        std::fs::rename(temporary, self.dir.join("checkpoint.bin"))?;
        File::open(&self.dir)?.sync_all()?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use transparent_filter::{build_filter, ScriptBytes};

    const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;
    const PROFILE: &str = transparent_filter::PROFILE;
    const START: u64 = 100;

    fn hash_at(height: u64) -> BlockHash {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&height.to_le_bytes());
        BlockHash::from_internal_bytes(bytes)
    }

    fn filter_for(height: u64) -> Vec<u8> {
        let elements = vec![ScriptBytes::new(vec![0x76, 0xa9, height as u8])];
        build_filter(hash_at(height), &elements).unwrap().0
    }

    fn open(dir: &Path) -> FilterStore {
        FilterStore::open(dir, PROFILE, GENESIS, START).expect("open")
    }

    fn fill(store: &mut FilterStore, through: u64) {
        for height in START..=through {
            store
                .append(height, hash_at(height), &filter_for(height))
                .unwrap();
        }
        store.commit().unwrap();
    }

    #[test]
    fn a_fresh_store_has_no_coverage() {
        let dir = tempfile::tempdir().unwrap();
        let store = open(dir.path());
        assert_eq!(store.covered_through(), None);
        assert_eq!(store.next_height(), START);
        assert_eq!(store.filters_stored(), 0);
    }

    #[test]
    fn appends_survive_reopening() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        fill(&mut store, START + 4);
        drop(store);

        let store = open(dir.path());
        assert_eq!(store.covered_through(), Some(START + 4));
        assert_eq!(store.filters_stored(), 5);
        assert_eq!(store.block_hash_at(START + 2), Some(hash_at(START + 2)));
        assert_eq!(store.height_of(hash_at(START + 3)), Some(START + 3));
        let stored = store.filter_by_hash(hash_at(START + 1)).unwrap().unwrap();
        assert_eq!(stored.bytes, filter_for(START + 1));
        assert_eq!(
            stored.digest,
            transparent_filter::filter_hash(&stored.bytes).0
        );
    }

    #[test]
    fn uncommitted_appends_are_discarded_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        fill(&mut store, START + 2);
        // Appended but never committed: a crash here must not leave coverage
        // claiming a filter the checkpoint does not cover.
        store
            .append(START + 3, hash_at(START + 3), &filter_for(START + 3))
            .unwrap();
        drop(store);

        let store = open(dir.path());
        assert_eq!(store.covered_through(), Some(START + 2));
        assert_eq!(store.filters_stored(), 3);
        assert!(store.filter_by_hash(hash_at(START + 3)).unwrap().is_none());
    }

    #[test]
    fn heights_must_be_contiguous() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        assert!(store
            .append(START + 1, hash_at(START + 1), &filter_for(1))
            .is_err());
        fill(&mut store, START);
        assert!(store
            .append(START + 5, hash_at(START + 5), &filter_for(5))
            .is_err());
    }

    #[test]
    fn rollback_drops_coverage_but_keeps_filter_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        fill(&mut store, START + 5);
        store.rollback_to(Some(START + 2)).unwrap();

        assert_eq!(store.covered_through(), Some(START + 2));
        assert_eq!(store.next_height(), START + 3);
        // Coverage is gone: the orphaned height no longer resolves.
        assert_eq!(store.block_hash_at(START + 4), None);
        assert_eq!(store.height_of(hash_at(START + 4)), None);
        // The immutable bytes remain cached under their own block hash.
        assert!(store.filter_by_hash(hash_at(START + 4)).unwrap().is_some());
    }

    #[test]
    fn a_replacement_branch_can_reuse_the_heights() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        fill(&mut store, START + 3);
        store.rollback_to(Some(START + 1)).unwrap();

        let replacement = BlockHash::from_internal_bytes([0xcc; 32]);
        let bytes = build_filter(replacement, &[]).unwrap().0;
        store.append(START + 2, replacement, &bytes).unwrap();
        store.commit().unwrap();

        assert_eq!(store.block_hash_at(START + 2), Some(replacement));
        assert_eq!(store.covered_through(), Some(START + 2));
    }

    #[test]
    fn a_block_returning_to_the_best_chain_reuses_its_stored_filter() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        fill(&mut store, START + 3);
        let before = store.filters_stored();
        store.rollback_to(Some(START + 1)).unwrap();
        // Re-accepting the same block must not store a second copy.
        store
            .append(START + 2, hash_at(START + 2), &filter_for(START + 2))
            .unwrap();
        store.commit().unwrap();
        assert_eq!(store.filters_stored(), before);
        assert_eq!(store.block_hash_at(START + 2), Some(hash_at(START + 2)));
    }

    #[test]
    fn the_same_block_hash_may_not_get_different_filter_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        fill(&mut store, START);
        store.rollback_to(Some(START - 1)).unwrap();
        let different = build_filter(hash_at(START), &[ScriptBytes::new(vec![0xff, 0xee])])
            .unwrap()
            .0;
        assert!(store.append(START, hash_at(START), &different).is_err());
    }

    #[test]
    fn rolling_back_everything_empties_coverage() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        fill(&mut store, START + 3);
        store.rollback_to(None).unwrap();
        assert_eq!(store.covered_through(), None);
        assert_eq!(store.next_height(), START);
    }

    #[test]
    fn an_incompatible_store_is_set_aside_rather_than_refusing_to_start() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        fill(&mut store, START + 1);
        drop(store);

        // A different profile must not be read as if it were this one.
        let store =
            FilterStore::open(dir.path(), "some-other-profile-v9", GENESIS, START).expect("open");
        assert_eq!(store.covered_through(), None);
        assert!(dir
            .path()
            .join("superseded-v1-zcash-transparent-basic-v1")
            .exists());
    }

    #[test]
    fn a_different_chain_identity_sets_the_store_aside() {
        let dir = tempfile::tempdir().unwrap();
        let mut store = open(dir.path());
        fill(&mut store, START);
        drop(store);

        let other_genesis = "00".repeat(32);
        let store = FilterStore::open(dir.path(), PROFILE, &other_genesis, START).expect("open");
        assert_eq!(store.covered_through(), None);
    }
}
