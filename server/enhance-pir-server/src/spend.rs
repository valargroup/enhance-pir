//! Reorg-aware transparent spend log and cold/warm PIR bucket tables.

use crate::coordinator::TableSource;
use crate::types::{DatabaseId, DatabaseLayout, TRANSPARENT_SPEND_LAYOUT};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use transparent_spend_pir::{
    bucket_count, bucket_for_outpoint, TransparentSpendEntry, BUCKET_CAPACITY, ENTRY_BYTES,
    ROW_BYTES, WARM_BLOCKS,
};

const INDEX_MAGIC: &[u8; 8] = b"TSPBLK01";
const BLOCK_BYTES: usize = 56;

#[derive(Clone, Debug)]
pub struct SpendBlock {
    pub height: u64,
    pub hash: String,
    pub first_position: u64,
    pub entry_count: u64,
}

/// A fixed-width append-only index avoids rewriting a multi-million-block JSON
/// manifest during the genesis-to-tip backfill.
pub struct SpendJournal {
    dir: PathBuf,
    entries_path: PathBuf,
    blocks_path: PathBuf,
    checkpoint_path: PathBuf,
    blocks: Vec<SpendBlock>,
}

impl SpendJournal {
    pub fn open(data_dir: &Path) -> Result<Self, std::io::Error> {
        let dir = data_dir.join("transparent-spends");
        fs::create_dir_all(&dir)?;
        let entries_path = dir.join("entries.bin");
        let blocks_path = dir.join("blocks.bin");
        let checkpoint_path = dir.join("checkpoint.bin");
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&entries_path)?;
        if !blocks_path.exists() {
            let mut file = File::create(&blocks_path)?;
            file.write_all(INDEX_MAGIC)?;
            file.sync_all()?;
        }
        if !checkpoint_path.exists() {
            let mut file = File::create(&checkpoint_path)?;
            file.write_all(&0u64.to_le_bytes())?;
            file.sync_all()?;
        }
        let mut encoded = Vec::new();
        File::open(&blocks_path)?.read_to_end(&mut encoded)?;
        if encoded.get(..INDEX_MAGIC.len()) != Some(INDEX_MAGIC) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "invalid transparent-spend block index",
            ));
        }
        let mut checkpoint = [0; 8];
        File::open(&checkpoint_path)?.read_exact(&mut checkpoint)?;
        let committed_blocks = usize::try_from(u64::from_le_bytes(checkpoint)).map_err(|_| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, "checkpoint exceeds usize")
        })?;
        let complete_blocks = (encoded.len() - INDEX_MAGIC.len()) / BLOCK_BYTES;
        if committed_blocks > complete_blocks {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "transparent-spend checkpoint exceeds block index",
            ));
        }
        encoded.truncate(INDEX_MAGIC.len() + committed_blocks * BLOCK_BYTES);
        OpenOptions::new()
            .write(true)
            .open(&blocks_path)?
            .set_len(encoded.len() as u64)?;
        let records = &encoded[INDEX_MAGIC.len()..];
        if !records.len().is_multiple_of(BLOCK_BYTES) {
            let complete = records.len() / BLOCK_BYTES;
            OpenOptions::new()
                .write(true)
                .open(&blocks_path)?
                .set_len((INDEX_MAGIC.len() + complete * BLOCK_BYTES) as u64)?;
            encoded.truncate(INDEX_MAGIC.len() + complete * BLOCK_BYTES);
        }
        let records = &encoded[INDEX_MAGIC.len()..];
        let mut blocks = Vec::with_capacity(records.len() / BLOCK_BYTES);
        for record in records.chunks_exact(BLOCK_BYTES) {
            let height = u64::from_le_bytes(record[0..8].try_into().expect("fixed"));
            let hash = hex::encode(&record[8..40]);
            let first_position = u64::from_le_bytes(record[40..48].try_into().expect("fixed"));
            let entry_count = u64::from_le_bytes(record[48..56].try_into().expect("fixed"));
            if blocks.last().is_some_and(|last: &SpendBlock| {
                last.height + 1 != height
                    || last.first_position + last.entry_count != first_position
            }) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "non-contiguous transparent-spend block index",
                ));
            }
            blocks.push(SpendBlock {
                height,
                hash,
                first_position,
                entry_count,
            });
        }
        let indexed_entries = blocks
            .last()
            .map_or(0, |block| block.first_position + block.entry_count);
        OpenOptions::new()
            .write(true)
            .open(&entries_path)?
            .set_len(indexed_entries * ENTRY_BYTES as u64)?;
        Ok(Self {
            dir,
            entries_path,
            blocks_path,
            checkpoint_path,
            blocks,
        })
    }

    pub fn committed_height(&self) -> Option<u64> {
        self.blocks.last().map(|block| block.height)
    }

    pub fn last_block(&self) -> Option<&SpendBlock> {
        self.blocks.last()
    }

    pub fn blocks(&self) -> &[SpendBlock] {
        &self.blocks
    }

    pub fn append_block(
        &mut self,
        height: u64,
        hash: String,
        entries: &[TransparentSpendEntry],
    ) -> Result<(), std::io::Error> {
        if self
            .blocks
            .last()
            .is_some_and(|previous| previous.height + 1 != height)
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "transparent-spend blocks must be contiguous",
            ));
        }
        let hash_bytes = hex::decode(&hash)
            .ok()
            .filter(|bytes| bytes.len() == 32)
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid block hash")
            })?;
        let first_position = self
            .blocks
            .last()
            .map_or(0, |block| block.first_position + block.entry_count);
        let mut entries_file = OpenOptions::new().append(true).open(&self.entries_path)?;
        for entry in entries {
            entries_file.write_all(&entry.to_bytes())?;
        }

        let mut index_record = [0; BLOCK_BYTES];
        index_record[0..8].copy_from_slice(&height.to_le_bytes());
        index_record[8..40].copy_from_slice(&hash_bytes);
        index_record[40..48].copy_from_slice(&first_position.to_le_bytes());
        index_record[48..56].copy_from_slice(&(entries.len() as u64).to_le_bytes());
        let mut blocks_file = OpenOptions::new().append(true).open(&self.blocks_path)?;
        blocks_file.write_all(&index_record)?;
        self.blocks.push(SpendBlock {
            height,
            hash,
            first_position,
            entry_count: entries.len() as u64,
        });
        Ok(())
    }

    pub fn sync(&self) -> Result<(), std::io::Error> {
        File::open(&self.entries_path)?.sync_all()?;
        File::open(&self.blocks_path)?.sync_all()?;
        let temporary = self.dir.join("checkpoint.bin.tmp");
        {
            let mut file = File::create(&temporary)?;
            file.write_all(&(self.blocks.len() as u64).to_le_bytes())?;
            file.sync_all()?;
        }
        fs::rename(temporary, &self.checkpoint_path)?;
        File::open(&self.dir)?.sync_all()
    }

    pub fn read_entries(
        &self,
        first: u64,
        count: usize,
    ) -> Result<Vec<TransparentSpendEntry>, std::io::Error> {
        let mut file = File::open(&self.entries_path)?;
        file.seek(SeekFrom::Start(first * ENTRY_BYTES as u64))?;
        let mut bytes = vec![0; count * ENTRY_BYTES];
        file.read_exact(&mut bytes)?;
        bytes
            .chunks_exact(ENTRY_BYTES)
            .map(|encoded| {
                TransparentSpendEntry::from_bytes(encoded).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "malformed transparent-spend entry",
                    )
                })
            })
            .collect()
    }

    pub fn rewind_to_height(&mut self, height: Option<u64>) -> Result<(), std::io::Error> {
        let keep = height.map_or(0, |height| {
            self.blocks.partition_point(|block| block.height <= height)
        });
        self.blocks.truncate(keep);
        let entries = self
            .blocks
            .last()
            .map_or(0, |block| block.first_position + block.entry_count);
        OpenOptions::new()
            .write(true)
            .open(&self.entries_path)?
            .set_len(entries * ENTRY_BYTES as u64)?;
        OpenOptions::new()
            .write(true)
            .open(&self.blocks_path)?
            .set_len((INDEX_MAGIC.len() + keep * BLOCK_BYTES) as u64)?;
        self.sync()
    }
}

pub struct BucketTable {
    table: DatabaseId,
    buckets: usize,
    rows: Vec<u8>,
}

impl BucketTable {
    pub fn build(table: DatabaseId, entries: &[TransparentSpendEntry]) -> Result<Self, String> {
        if !matches!(
            table,
            DatabaseId::TransparentSpendCold | DatabaseId::TransparentSpendWarm
        ) {
            return Err("not a transparent spend table".to_string());
        }
        let mut buckets = bucket_count(entries.len() as u64);
        loop {
            let mut rows = vec![0; buckets * ROW_BYTES];
            let mut fill = vec![0u8; buckets];
            let mut overflow = false;
            for entry in entries {
                let bucket =
                    bucket_for_outpoint(&entry.outpoint_txid, entry.outpoint_index, buckets)
                        .expect("validated bucket count");
                let slot = fill[bucket] as usize;
                if slot == BUCKET_CAPACITY {
                    overflow = true;
                    break;
                }
                let start = bucket * ROW_BYTES + slot * ENTRY_BYTES;
                rows[start..start + ENTRY_BYTES].copy_from_slice(&entry.to_bytes());
                fill[bucket] += 1;
            }
            if !overflow {
                return Ok(Self {
                    table,
                    buckets,
                    rows,
                });
            }
            buckets = buckets
                .checked_mul(2)
                .ok_or_else(|| "transparent spend table size overflow".to_string())?;
        }
    }

    pub fn buckets(&self) -> usize {
        self.buckets
    }
}

impl TableSource for BucketTable {
    fn table(&self) -> DatabaseId {
        self.table
    }

    fn layout(&self) -> DatabaseLayout {
        TRANSPARENT_SPEND_LAYOUT
    }

    fn positions(&self) -> u64 {
        self.buckets as u64
    }

    fn shard_ids(&self) -> std::ops::RangeInclusive<u64> {
        0..=((self.buckets - 1) / TRANSPARENT_SPEND_LAYOUT.shard_rows) as u64
    }

    fn read_shard_rows(&self, shard_id: u64) -> Result<Vec<u8>, String> {
        let start = shard_id as usize * TRANSPARENT_SPEND_LAYOUT.shard_rows;
        if start >= self.buckets {
            return Err(format!("shard {shard_id} is outside the table"));
        }
        let end = (start + TRANSPARENT_SPEND_LAYOUT.shard_rows).min(self.buckets);
        let mut shard = vec![0; TRANSPARENT_SPEND_LAYOUT.shard_bytes()];
        let source = &self.rows[start * ROW_BYTES..end * ROW_BYTES];
        shard[..source.len()].copy_from_slice(source);
        Ok(shard)
    }

    fn populated_positions_in_shard(&self, shard_id: u64) -> u64 {
        let start = shard_id as usize * TRANSPARENT_SPEND_LAYOUT.shard_rows;
        self.buckets
            .saturating_sub(start)
            .min(TRANSPARENT_SPEND_LAYOUT.shard_rows) as u64
    }
}

pub struct TransparentSpendTables {
    pub cold_end_height: u64,
    pub cold: BucketTable,
    pub warm: BucketTable,
}

impl TransparentSpendTables {
    pub fn build(log: &SpendJournal, tip_height: u64) -> Result<Self, String> {
        let cold_end_height = tip_height.saturating_sub(WARM_BLOCKS);
        let cold_blocks = log
            .blocks()
            .partition_point(|block| block.height <= cold_end_height);
        let cold_count = log.blocks().get(cold_blocks).map_or_else(
            || {
                log.blocks()
                    .last()
                    .map_or(0, |block| block.first_position + block.entry_count)
            },
            |block| block.first_position,
        );
        let total_count = log
            .blocks()
            .last()
            .map_or(0, |block| block.first_position + block.entry_count);
        let cold = log
            .read_entries(0, cold_count as usize)
            .map_err(|error| error.to_string())?;
        let warm = log
            .read_entries(cold_count, (total_count - cold_count) as usize)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            cold_end_height,
            cold: BucketTable::build(DatabaseId::TransparentSpendCold, &cold)?,
            warm: BucketTable::build(DatabaseId::TransparentSpendWarm, &warm)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(height: u32) -> TransparentSpendEntry {
        TransparentSpendEntry {
            outpoint_txid: [height as u8; 32],
            outpoint_index: height,
            spending_txid: [9; 32],
            spend_height: height,
            transaction_index: 1,
        }
    }

    #[test]
    fn partitions_at_the_trailing_100000_blocks_and_rewinds() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SpendJournal::open(dir.path()).unwrap();
        log.append_block(1, "aa".repeat(32), &[entry(1)]).unwrap();
        log.append_block(2, "bb".repeat(32), &[entry(2)]).unwrap();
        let tables = TransparentSpendTables::build(&log, 100_001).unwrap();
        assert_eq!(tables.cold_end_height, 1);
        assert_eq!(tables.cold.buckets(), 8_192);
        assert_eq!(tables.warm.buckets(), 8_192);
        log.rewind_to_height(Some(1)).unwrap();
        assert_eq!(log.committed_height(), Some(1));
        assert_eq!(log.read_entries(0, 1).unwrap(), vec![entry(1)]);
    }

    #[test]
    fn restart_discards_an_uncheckpointed_tail() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = SpendJournal::open(dir.path()).unwrap();
        log.append_block(1, "aa".repeat(32), &[entry(1)]).unwrap();
        log.sync().unwrap();
        log.append_block(2, "bb".repeat(32), &[entry(2)]).unwrap();
        drop(log);

        let log = SpendJournal::open(dir.path()).unwrap();
        assert_eq!(log.committed_height(), Some(1));
        assert_eq!(log.read_entries(0, 1).unwrap(), vec![entry(1)]);
    }
}
