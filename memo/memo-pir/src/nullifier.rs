//! The nullifier tables: hash-bucketed membership over the spend log.
//!
//! A nullifier reveals nothing about the position of the note it spends, so
//! membership is keyed by `hash(nf)`, and every bucket can change with every
//! block. The tables are therefore *built* from the append-only spend log
//! rather than journaled: the cold table holds every spend up to the cold
//! checkpoint and is rebuilt only when the checkpoint moves; the warm table
//! holds the rest and is rebuilt every generation. A client always queries
//! both, so the split leaks nothing.

use crate::coordinator::TableSource;
use crate::ingest::{NullifierEntry, NULLIFIER_ENTRY_BYTES};
use crate::store::RecordJournal;
use crate::types::{DatabaseId, DatabaseLayout, NULLIFIER_LAYOUT};

/// Entries one bucket row holds.
pub const BUCKET_CAPACITY: usize = 112;
pub const BUCKET_BYTES: usize = BUCKET_CAPACITY * NULLIFIER_ENTRY_BYTES;
/// Target load of the cold table when it is sized.
pub const COLD_TARGET_LOAD: f64 = 0.55;
/// Fixed bucket count of the warm table (one shard, padded).
pub const WARM_BUCKETS: usize = 2_048;
/// Smallest cold table: one shard of rows.
pub const MIN_COLD_BUCKETS: usize = NULLIFIER_LAYOUT.shard_rows;

/// Bucket (row) a nullifier lives in. Nullifiers are PRF outputs, so the
/// first four bytes are uniform.
pub fn hash_to_bucket(nullifier: &[u8; 32], num_buckets: usize) -> usize {
    let prefix = u32::from_le_bytes(nullifier[..4].try_into().expect("four bytes"));
    (prefix as usize) % num_buckets
}

/// Power-of-two bucket count keeping the expected load at or below the
/// target, never below one shard.
pub fn cold_bucket_count(entries: u64) -> usize {
    let needed = (entries as f64 / (COLD_TARGET_LOAD * BUCKET_CAPACITY as f64)).ceil() as usize;
    needed.max(MIN_COLD_BUCKETS).next_power_of_two()
}

/// Finds `nullifier` in one decoded bucket row.
pub fn scan_bucket(row: &[u8], nullifier: &[u8; 32]) -> Option<NullifierEntry> {
    row.get(..BUCKET_BYTES)?
        .chunks_exact(NULLIFIER_ENTRY_BYTES)
        .filter(|entry| &entry[..32] == nullifier)
        .find_map(NullifierEntry::from_bytes)
}

/// One built table: `num_buckets` rows of `BUCKET_BYTES`, empty slots zero.
pub struct BucketTable {
    table: DatabaseId,
    num_buckets: usize,
    rows: Vec<u8>,
    entries: u64,
}

impl BucketTable {
    pub fn build<I: IntoIterator<Item = NullifierEntry>>(
        table: DatabaseId,
        num_buckets: usize,
        entries: I,
    ) -> Result<Self, String> {
        if !matches!(table, DatabaseId::NfCold | DatabaseId::NfWarm) {
            return Err(format!("{table} is not a nullifier table"));
        }
        if num_buckets == 0 || !num_buckets.is_power_of_two() {
            return Err("bucket count must be a power of two".to_string());
        }
        let mut rows = vec![0u8; num_buckets * BUCKET_BYTES];
        let mut fill = vec![0u16; num_buckets];
        let mut count = 0u64;
        for entry in entries {
            let bucket = hash_to_bucket(&entry.nullifier, num_buckets);
            let slot = fill[bucket] as usize;
            if slot >= BUCKET_CAPACITY {
                return Err(format!(
                    "{table} bucket {bucket} overflows {BUCKET_CAPACITY} entries; resize the table"
                ));
            }
            let start = bucket * BUCKET_BYTES + slot * NULLIFIER_ENTRY_BYTES;
            rows[start..start + NULLIFIER_ENTRY_BYTES].copy_from_slice(&entry.to_bytes());
            fill[bucket] += 1;
            count += 1;
        }
        Ok(Self {
            table,
            num_buckets,
            rows,
            entries: count,
        })
    }

    pub fn num_buckets(&self) -> usize {
        self.num_buckets
    }

    pub fn entries(&self) -> u64 {
        self.entries
    }

    /// Looks a nullifier up directly (operators and tests; clients use PIR).
    pub fn lookup(&self, nullifier: &[u8; 32]) -> Option<NullifierEntry> {
        let bucket = hash_to_bucket(nullifier, self.num_buckets);
        scan_bucket(
            &self.rows[bucket * BUCKET_BYTES..(bucket + 1) * BUCKET_BYTES],
            nullifier,
        )
    }
}

impl TableSource for BucketTable {
    fn table(&self) -> DatabaseId {
        self.table
    }

    fn layout(&self) -> DatabaseLayout {
        NULLIFIER_LAYOUT
    }

    fn positions(&self) -> u64 {
        self.num_buckets as u64
    }

    fn shard_ids(&self) -> std::ops::RangeInclusive<u64> {
        0..=((self.num_buckets - 1) / NULLIFIER_LAYOUT.shard_rows) as u64
    }

    fn read_shard_rows(&self, shard_id: u64) -> Result<Vec<u8>, String> {
        let layout = NULLIFIER_LAYOUT;
        let start_row = shard_id as usize * layout.shard_rows;
        if start_row >= self.num_buckets {
            return Err(format!("shard {shard_id} is outside the table"));
        }
        let end_row = (start_row + layout.shard_rows).min(self.num_buckets);
        let mut rows = vec![0u8; layout.shard_bytes()];
        let bytes = &self.rows[start_row * BUCKET_BYTES..end_row * BUCKET_BYTES];
        rows[..bytes.len()].copy_from_slice(bytes);
        Ok(rows)
    }

    fn populated_positions_in_shard(&self, shard_id: u64) -> u64 {
        let layout = NULLIFIER_LAYOUT;
        let start_row = shard_id as usize * layout.shard_rows;
        (self.num_buckets.saturating_sub(start_row)).min(layout.shard_rows) as u64
    }
}

/// Both tables built from the spend log at one checkpoint.
pub struct NullifierTables {
    pub checkpoint: u64,
    pub cold: BucketTable,
    pub warm: BucketTable,
}

impl NullifierTables {
    /// Reads every logged spend and splits it at `checkpoint` (inclusive on
    /// the cold side).
    pub fn build(log: &RecordJournal, checkpoint: u64) -> Result<Self, String> {
        let total = log.tree_size();
        let mut cold = Vec::new();
        let mut warm = Vec::new();
        const CHUNK: u64 = 65_536;
        let mut start = 0u64;
        while start < total {
            let count = (total - start).min(CHUNK) as usize;
            let bytes = log.read_records(start, count).map_err(|e| e.to_string())?;
            for chunk in bytes.chunks_exact(NULLIFIER_ENTRY_BYTES) {
                let entry = NullifierEntry::from_bytes(chunk)
                    .ok_or_else(|| "malformed nullifier log entry".to_string())?;
                if u64::from(entry.spend_height) <= checkpoint {
                    cold.push(entry);
                } else {
                    warm.push(entry);
                }
            }
            start += count as u64;
        }
        let cold_buckets = cold_bucket_count(cold.len() as u64);
        Ok(Self {
            checkpoint,
            cold: BucketTable::build(DatabaseId::NfCold, cold_buckets, cold)?,
            warm: BucketTable::build(DatabaseId::NfWarm, WARM_BUCKETS, warm)?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(seed: u64, height: u32) -> NullifierEntry {
        let mut nullifier = [0u8; 32];
        nullifier[..8].copy_from_slice(&seed.to_le_bytes());
        nullifier[8..16].copy_from_slice(&seed.wrapping_mul(0x9e37_79b9_7f4a_7c15).to_le_bytes());
        NullifierEntry {
            nullifier,
            spend_height: height,
            first_output_position: (seed * 3) as u32,
            action_count: 2,
        }
    }

    #[test]
    fn cold_sizing_keeps_load_under_target_and_never_below_a_shard() {
        assert_eq!(cold_bucket_count(0), MIN_COLD_BUCKETS);
        assert_eq!(cold_bucket_count(100_000), MIN_COLD_BUCKETS);
        let buckets = cold_bucket_count(1_000_000);
        assert!(buckets.is_power_of_two());
        assert!(1_000_000.0 / (buckets as f64 * BUCKET_CAPACITY as f64) <= COLD_TARGET_LOAD);
        assert_eq!(buckets, 16_384);
    }

    #[test]
    fn built_table_answers_lookups_and_pads_shards() {
        let entries: Vec<_> = (0..5_000u64).map(|i| entry(i, 10)).collect();
        let table = BucketTable::build(DatabaseId::NfWarm, WARM_BUCKETS, entries.clone()).unwrap();
        assert_eq!(table.entries(), 5_000);
        assert_eq!(
            table.lookup(&entries[4_321].nullifier),
            Some(entries[4_321])
        );
        assert_eq!(table.lookup(&entry(99_999, 1).nullifier), None);
        assert_eq!(table.shard_ids(), 0..=0);
        let rows = table.read_shard_rows(0).unwrap();
        assert_eq!(rows.len(), NULLIFIER_LAYOUT.shard_bytes());
        assert!(rows[WARM_BUCKETS * BUCKET_BYTES..].iter().all(|b| *b == 0));
        // The row a client would decode holds the entry at its bucket.
        let bucket = hash_to_bucket(&entries[7].nullifier, WARM_BUCKETS);
        assert_eq!(
            scan_bucket(
                &rows[bucket * BUCKET_BYTES..(bucket + 1) * BUCKET_BYTES],
                &entries[7].nullifier
            ),
            Some(entries[7])
        );
    }

    #[test]
    fn overflowing_a_bucket_is_an_error_not_a_silent_drop() {
        let same_bucket = (0..BUCKET_CAPACITY as u64 + 1).map(|i| {
            let mut e = entry(i, 1);
            e.nullifier[..4].copy_from_slice(&[7, 0, 0, 0]);
            e
        });
        assert!(BucketTable::build(DatabaseId::NfCold, MIN_COLD_BUCKETS, same_bucket).is_err());
    }
}
