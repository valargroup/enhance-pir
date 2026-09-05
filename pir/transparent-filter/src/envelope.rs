//! The range-delivery envelope.
//!
//! This application format is versioned separately from BIP 158. The filter
//! bytes it carries are opaque and are never reframed, so batching a range
//! differently cannot change a single filter byte.
//!
//! Every hash is serialized in internal byte order. Display hex exists only at
//! the presentation boundary and never appears in binary serialization.

use crate::error::FilterError;
use crate::hash::BlockHash;

/// Envelope format version. Independent of the BIP 158 encoding version.
pub const ENVELOPE_VERSION: u16 = 1;

/// Four-byte magic: "ZTFB", Zcash transparent filter batch.
pub const MAGIC: [u8; 4] = *b"ZTFB";

/// Maximum records in one batch. An application limit, not a format limit: a
/// longer range is fetched as several batches.
pub const MAX_RECORDS_PER_BATCH: u64 = 1_000;

/// Ceiling on a batch's serialized size, applied before allocation.
pub const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;

/// One block's filter, as delivered.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterRecord {
    pub height: u64,
    pub block_hash: BlockHash,
    pub filter: Vec<u8>,
}

/// An ordered run of filters covering a requested range.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterBatch {
    pub version: u16,
    /// Chain identity: the genesis block hash, in internal byte order.
    pub genesis: BlockHash,
    pub profile: String,
    pub start_height: u64,
    /// The block the range terminates at, which the caller already accepts.
    pub stop_block_hash: BlockHash,
    pub records: Vec<FilterRecord>,
}

fn write_compact_size(out: &mut Vec<u8>, value: u64) {
    match value {
        0..=0xfc => out.push(value as u8),
        0xfd..=0xffff => {
            out.push(0xfd);
            out.extend_from_slice(&(value as u16).to_le_bytes());
        }
        0x1_0000..=0xffff_ffff => {
            out.push(0xfe);
            out.extend_from_slice(&(value as u32).to_le_bytes());
        }
        _ => {
            out.push(0xff);
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
}

/// Cursor over a byte slice that never panics and never over-reads.
struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn take(&mut self, count: usize) -> Result<&'a [u8], FilterError> {
        let end = self
            .offset
            .checked_add(count)
            .ok_or_else(|| FilterError::Envelope("length overflow".into()))?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| FilterError::Envelope("envelope is truncated".into()))?;
        self.offset = end;
        Ok(slice)
    }

    fn u16(&mut self) -> Result<u16, FilterError> {
        Ok(u16::from_le_bytes(
            self.take(2)?.try_into().expect("2 bytes"),
        ))
    }

    fn u64(&mut self) -> Result<u64, FilterError> {
        Ok(u64::from_le_bytes(
            self.take(8)?.try_into().expect("8 bytes"),
        ))
    }

    fn hash(&mut self) -> Result<BlockHash, FilterError> {
        Ok(BlockHash::from_internal_bytes(
            self.take(32)?.try_into().expect("32 bytes"),
        ))
    }

    /// Reads a CompactSize, rejecting noncanonical encodings.
    fn compact_size(&mut self) -> Result<u64, FilterError> {
        let first = self.take(1)?[0];
        let (value, minimum) = match first {
            0xfd => (
                u64::from(u16::from_le_bytes(self.take(2)?.try_into().expect("2"))),
                0xfd,
            ),
            0xfe => (
                u64::from(u32::from_le_bytes(self.take(4)?.try_into().expect("4"))),
                0x1_0000,
            ),
            0xff => (
                u64::from_le_bytes(self.take(8)?.try_into().expect("8")),
                0x1_0000_0000,
            ),
            value => (u64::from(value), 0),
        };
        if value < minimum {
            return Err(FilterError::Envelope(
                "CompactSize is not canonically encoded".into(),
            ));
        }
        Ok(value)
    }

    fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }
}

impl FilterBatch {
    /// Serializes the batch. Filter bytes are copied verbatim.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend_from_slice(&MAGIC);
        out.extend_from_slice(&self.version.to_le_bytes());
        out.extend_from_slice(self.genesis.internal_bytes());
        write_compact_size(&mut out, self.profile.len() as u64);
        out.extend_from_slice(self.profile.as_bytes());
        out.extend_from_slice(&self.start_height.to_le_bytes());
        out.extend_from_slice(self.stop_block_hash.internal_bytes());
        write_compact_size(&mut out, self.records.len() as u64);
        for record in &self.records {
            out.extend_from_slice(&record.height.to_le_bytes());
            out.extend_from_slice(record.block_hash.internal_bytes());
            write_compact_size(&mut out, record.filter.len() as u64);
            out.extend_from_slice(&record.filter);
        }
        out
    }

    /// Parses a batch, applying size and count limits before allocation.
    ///
    /// This checks only that the bytes are a well formed batch. Whether the
    /// batch is the one that was asked for — right chain, right profile, right
    /// heights on the caller's own accepted chain — is checked separately by
    /// the client, because those answers depend on wallet state this function
    /// does not have.
    pub fn decode(bytes: &[u8]) -> Result<Self, FilterError> {
        if bytes.len() > MAX_BATCH_BYTES {
            return Err(FilterError::Envelope(format!(
                "batch is {} bytes, limit is {MAX_BATCH_BYTES}",
                bytes.len()
            )));
        }
        let mut reader = Reader::new(bytes);
        if reader.take(4)? != MAGIC {
            return Err(FilterError::Envelope("bad magic".into()));
        }
        let version = reader.u16()?;
        if version != ENVELOPE_VERSION {
            return Err(FilterError::Envelope(format!(
                "unsupported envelope version {version}"
            )));
        }
        let genesis = reader.hash()?;
        let profile_len = reader.compact_size()?;
        if profile_len > 64 {
            return Err(FilterError::Envelope("profile name is too long".into()));
        }
        let profile = std::str::from_utf8(reader.take(profile_len as usize)?)
            .map_err(|_| FilterError::Envelope("profile name is not UTF-8".into()))?
            .to_string();
        let start_height = reader.u64()?;
        let stop_block_hash = reader.hash()?;
        let count = reader.compact_size()?;
        if count > MAX_RECORDS_PER_BATCH {
            return Err(FilterError::Envelope(format!(
                "batch claims {count} records, limit is {MAX_RECORDS_PER_BATCH}"
            )));
        }
        // Every record costs at least 8 + 32 + 1 bytes, so a count that cannot
        // fit in what remains is refused before the vector is reserved.
        if count.saturating_mul(41) > reader.remaining() as u64 {
            return Err(FilterError::Envelope(format!(
                "batch claims {count} records but has {} bytes left",
                reader.remaining()
            )));
        }
        let mut records = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let height = reader.u64()?;
            let block_hash = reader.hash()?;
            let length = reader.compact_size()?;
            if length > reader.remaining() as u64 {
                return Err(FilterError::Envelope("record length exceeds batch".into()));
            }
            records.push(FilterRecord {
                height,
                block_hash,
                filter: reader.take(length as usize)?.to_vec(),
            });
        }
        if reader.remaining() != 0 {
            return Err(FilterError::Envelope(format!(
                "{} trailing bytes after the batch",
                reader.remaining()
            )));
        }
        Ok(Self {
            version,
            genesis,
            profile,
            start_height,
            stop_block_hash,
            records,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::{MAINNET_GENESIS_DISPLAY, PROFILE};

    fn batch(records: Vec<FilterRecord>) -> FilterBatch {
        FilterBatch {
            version: ENVELOPE_VERSION,
            genesis: BlockHash::from_display_hex(MAINNET_GENESIS_DISPLAY).unwrap(),
            profile: PROFILE.to_string(),
            start_height: 3_428_143,
            stop_block_hash: BlockHash::from_internal_bytes([0xab; 32]),
            records,
        }
    }

    fn record(height: u64, filter: Vec<u8>) -> FilterRecord {
        FilterRecord {
            height,
            block_hash: BlockHash::from_internal_bytes([height as u8; 32]),
            filter,
        }
    }

    #[test]
    fn round_trips_including_the_empty_batch_and_empty_filters() {
        for records in [
            vec![],
            vec![record(1, vec![0x00])],
            vec![record(1, vec![0x00]), record(2, vec![0x02, 0xaa, 0xbb])],
        ] {
            let original = batch(records);
            let decoded = FilterBatch::decode(&original.encode()).expect("decode");
            assert_eq!(original, decoded);
        }
    }

    #[test]
    fn truncation_at_every_offset_is_rejected() {
        let bytes = batch(vec![record(1, vec![0x00]), record(2, vec![0x01, 0xff])]).encode();
        for cut in 0..bytes.len() {
            assert!(
                FilterBatch::decode(&bytes[..cut]).is_err(),
                "prefix of {cut} bytes decoded"
            );
        }
        assert!(FilterBatch::decode(&bytes).is_ok());
    }

    #[test]
    fn trailing_bytes_are_rejected() {
        let mut bytes = batch(vec![record(1, vec![0x00])]).encode();
        bytes.push(0);
        assert!(matches!(
            FilterBatch::decode(&bytes),
            Err(FilterError::Envelope(_))
        ));
    }

    #[test]
    fn bad_magic_and_version_are_rejected() {
        let mut bytes = batch(vec![]).encode();
        bytes[0] = b'X';
        assert!(FilterBatch::decode(&bytes).is_err());

        let mut bytes = batch(vec![]).encode();
        bytes[4] = 9;
        assert!(FilterBatch::decode(&bytes).is_err());
    }

    #[test]
    fn an_oversized_record_count_is_refused_before_allocation() {
        let mut bytes = batch(vec![]).encode();
        // Replace the trailing zero record count with a claim of 1000 records.
        bytes.pop();
        bytes.push(0xfd);
        bytes.extend_from_slice(&1000u16.to_le_bytes());
        assert!(matches!(
            FilterBatch::decode(&bytes),
            Err(FilterError::Envelope(_))
        ));
    }

    #[test]
    fn a_count_beyond_the_batch_limit_is_refused() {
        let mut bytes = batch(vec![]).encode();
        bytes.pop();
        bytes.push(0xfd);
        bytes.extend_from_slice(&2000u16.to_le_bytes());
        let error = FilterBatch::decode(&bytes).unwrap_err();
        assert!(format!("{error}").contains("limit is 1000"));
    }

    #[test]
    fn hashes_are_serialized_in_internal_order_not_display_order() {
        let encoded = batch(vec![]).encode();
        let genesis = BlockHash::from_display_hex(MAINNET_GENESIS_DISPLAY).unwrap();
        assert_eq!(&encoded[6..38], genesis.internal_bytes());
        // The display string's leading zeros must not appear at the front.
        assert_ne!(&encoded[6..10], &[0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn batching_does_not_alter_filter_bytes() {
        let filter = vec![0x03, 0xde, 0xad, 0xbe, 0xef];
        let alone = batch(vec![record(7, filter.clone())]);
        let grouped = batch(vec![
            record(6, vec![0x00]),
            record(7, filter.clone()),
            record(8, vec![0x00]),
        ]);
        let from_alone = FilterBatch::decode(&alone.encode()).unwrap();
        let from_grouped = FilterBatch::decode(&grouped.encode()).unwrap();
        assert_eq!(from_alone.records[0].filter, filter);
        assert_eq!(from_grouped.records[1].filter, filter);
    }
}
