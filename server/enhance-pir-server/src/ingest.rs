//! Append finalized Ironwood enhancement records to the active journal.

use crate::store::{RecordJournal, StoreError};
use crate::types::{DatabaseId, ENHANCE_LAYOUT};
use crate::zakura::CanonicalBlock;
use std::path::Path;

#[derive(Debug, thiserror::Error)]
pub enum IngestError {
    #[error("journal error: {0}")]
    Store(#[from] StoreError),
    #[error("ingest invariant violated: {0}")]
    Invariant(String),
}

pub struct EnhanceJournal {
    pub records: RecordJournal,
}

impl EnhanceJournal {
    pub fn open(data_dir: &Path) -> Result<Self, IngestError> {
        Ok(Self {
            records: RecordJournal::open(
                data_dir.join(DatabaseId::Enhance.as_str()),
                DatabaseId::Enhance,
                ENHANCE_LAYOUT,
            )?,
        })
    }

    pub fn committed_height(&self) -> Option<u64> {
        self.records.last_block().map(|block| block.height)
    }

    pub fn highest_committed(&self) -> Option<(u64, String)> {
        self.records
            .last_block()
            .map(|block| (block.height, block.hash.clone()))
    }

    pub fn append_block(&mut self, block: &CanonicalBlock) -> Result<(), IngestError> {
        if self
            .records
            .last_block()
            .is_some_and(|last| last.height >= block.height)
        {
            return Ok(());
        }
        let first_position = block
            .tree_size
            .checked_sub(block.records.len() as u64)
            .ok_or_else(|| IngestError::Invariant("block exceeds tree size".to_string()))?;
        if self.records.tree_size() < first_position || self.records.tree_size() > block.tree_size {
            return Err(IngestError::Invariant(format!(
                "journal continuity mismatch at height {}: journal {}, block {}..{}",
                block.height,
                self.records.tree_size(),
                first_position,
                block.tree_size
            )));
        }
        let skip = (self.records.tree_size() - first_position) as usize;
        self.records
            .append_block(block.height, block.hash.clone(), &block.records[skip..])?;
        if self.records.tree_size() != block.tree_size {
            return Err(IngestError::Invariant(format!(
                "Ironwood tree size mismatch at height {}: journal {}, Zakura {}",
                block.height,
                self.records.tree_size(),
                block.tree_size
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use enhance_pir::EnhanceRecord;

    #[test]
    fn appends_only_the_active_enhancement_journal() {
        let directory = tempfile::tempdir().unwrap();
        let mut journal = EnhanceJournal::open(directory.path()).unwrap();
        let block = CanonicalBlock {
            height: 3_428_143,
            hash: "01".repeat(32),
            records: vec![EnhanceRecord([7; enhance_pir::RECORD_BYTES])],
            tree_size: 1,
        };
        journal.append_block(&block).unwrap();
        assert_eq!(journal.records.tree_size(), 1);
        assert_eq!(journal.committed_height(), Some(3_428_143));
        assert!(directory.path().join("enhance/records.bin").exists());
        assert!(!directory.path().join("action").exists());
    }
}
