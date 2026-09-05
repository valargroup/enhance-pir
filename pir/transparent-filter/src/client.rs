//! Wallet-side range validation and matching.
//!
//! The wallet chooses a contiguous range from its own durable checkpoint to a
//! block it already accepts, and asks for all of it. It does not ask only for
//! the blocks that matched, because the set of blocks it asks about would then
//! be a function of its own scripts.

use crate::envelope::{FilterBatch, MAX_RECORDS_PER_BATCH};
use crate::error::FilterError;
use crate::hash::BlockHash;
use crate::matching::{map_wallet_scripts, match_mapped};
use crate::script::ScriptBytes;
use crate::transport::{ByteCharges, FilterTransport, RangeRequest};
use crate::validate::{validate_filter, FilterLimits, ValidatedFilter};

/// The wallet's own view of the accepted chain.
///
/// Height-to-hash comes from the wallet, never from the batch being checked. A
/// server that could supply both the filters and the chain they claim to be on
/// could place any filter anywhere.
pub trait AcceptedChain {
    /// The accepted block hash at `height`, if the wallet has accepted one.
    fn block_hash(&self, height: u64) -> Option<BlockHash>;
    /// The height of an accepted block, if it is on the accepted chain.
    fn height_of(&self, hash: BlockHash) -> Option<u64>;
}

/// An in-memory accepted chain, for tests and for a caller that already holds
/// a height-to-hash map.
#[derive(Clone, Debug, Default)]
pub struct ChainMap {
    by_height: std::collections::BTreeMap<u64, BlockHash>,
}

impl ChainMap {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&mut self, height: u64, hash: BlockHash) -> &mut Self {
        self.by_height.insert(height, hash);
        self
    }

    /// Drops every entry above `height`, as a chain replacement requires.
    pub fn rollback_to(&mut self, height: u64) {
        self.by_height.retain(|at, _| *at <= height);
    }

    pub fn tip(&self) -> Option<(u64, BlockHash)> {
        self.by_height
            .iter()
            .next_back()
            .map(|(h, hash)| (*h, *hash))
    }
}

impl AcceptedChain for ChainMap {
    fn block_hash(&self, height: u64) -> Option<BlockHash> {
        self.by_height.get(&height).copied()
    }
    fn height_of(&self, hash: BlockHash) -> Option<u64> {
        self.by_height
            .iter()
            .find(|(_, candidate)| **candidate == hash)
            .map(|(height, _)| *height)
    }
}

/// A record whose filter has been fully validated and located on the accepted
/// chain.
#[derive(Clone, Debug)]
pub struct CheckedRecord {
    pub height: u64,
    pub block_hash: BlockHash,
    pub filter: ValidatedFilter,
}

/// Checks a batch against the request and the wallet's accepted chain.
///
/// Rejects a batch that is on the wrong chain or profile, out of order, short,
/// long, duplicated, or anchored to blocks the wallet has not accepted. Filters
/// are fully validated here, so a caller holding the result never has to
/// remember whether validation happened.
pub fn check_batch(
    batch: &FilterBatch,
    request: &RangeRequest,
    chain: &impl AcceptedChain,
    limits: FilterLimits,
) -> Result<Vec<CheckedRecord>, FilterError> {
    if batch.genesis != request.genesis {
        return Err(FilterError::Response(
            "batch is for a different chain".into(),
        ));
    }
    if batch.profile != request.profile {
        return Err(FilterError::Response(format!(
            "batch profile {:?} is not the requested {:?}",
            batch.profile, request.profile
        )));
    }
    if batch.start_height != request.start_height {
        return Err(FilterError::Response(format!(
            "batch starts at {}, requested {}",
            batch.start_height, request.start_height
        )));
    }
    if batch.stop_block_hash != request.stop_block_hash {
        return Err(FilterError::Response(
            "batch terminates at a different block".into(),
        ));
    }

    let stop_height = chain.height_of(request.stop_block_hash).ok_or_else(|| {
        FilterError::Response("terminal block is not on the accepted chain".into())
    })?;
    if stop_height < request.start_height {
        return Err(FilterError::Response(
            "terminal block is below the requested start".into(),
        ));
    }
    let wanted = (stop_height - request.start_height + 1).min(MAX_RECORDS_PER_BATCH);
    if batch.records.len() as u64 != wanted {
        return Err(FilterError::Response(format!(
            "batch has {} records, expected exactly {wanted}",
            batch.records.len()
        )));
    }

    let mut checked = Vec::with_capacity(batch.records.len());
    for (offset, record) in batch.records.iter().enumerate() {
        let expected_height = request.start_height + offset as u64;
        // Contiguity and ordering together rule out gaps, duplicates and
        // reordering: each record must sit at exactly its position's height.
        if record.height != expected_height {
            return Err(FilterError::Response(format!(
                "record {offset} is at height {}, expected {expected_height}",
                record.height
            )));
        }
        let accepted = chain.block_hash(record.height).ok_or_else(|| {
            FilterError::Response(format!(
                "wallet has not accepted a block at height {}",
                record.height
            ))
        })?;
        if accepted != record.block_hash {
            return Err(FilterError::Response(format!(
                "record at height {} is on a different branch",
                record.height
            )));
        }
        let filter = validate_filter(&record.filter, limits)?;
        checked.push(CheckedRecord {
            height: record.height,
            block_hash: record.block_hash,
            filter,
        });
    }
    Ok(checked)
}

/// One block where at least one wallet script matched.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMatch {
    pub height: u64,
    pub block_hash: BlockHash,
    /// Indices into the caller's script list. Every match, not just the first.
    pub script_indices: Vec<usize>,
}

/// Outcome of synchronizing a range.
#[derive(Clone, Debug)]
pub struct SyncOutcome {
    /// Blocks with at least one match, ascending by height.
    pub matches: Vec<BlockMatch>,
    /// The highest height whose filter was validated and matched.
    pub covered_through: u64,
    pub covered_block_hash: BlockHash,
    /// Every byte the transport actually delivered.
    pub charges: ByteCharges,
    /// Filters validated, matched or not.
    pub filters_checked: u64,
}

/// Fetches, validates and matches a contiguous range in bounded batches.
///
/// Coverage is reported only for the prefix that was fully validated. A caller
/// commits its checkpoint from `covered_through` only after whatever private
/// work the matches imply is durably complete; committing earlier would turn an
/// interruption into a silent coverage gap.
pub fn sync_range(
    transport: &mut impl FilterTransport,
    request: &RangeRequest,
    chain: &impl AcceptedChain,
    wallet_scripts: &[ScriptBytes],
    limits: FilterLimits,
) -> Result<SyncOutcome, FilterError> {
    let stop_height = chain.height_of(request.stop_block_hash).ok_or_else(|| {
        FilterError::Response("terminal block is not on the accepted chain".into())
    })?;
    let mut charges = ByteCharges::default();
    let mut matches = Vec::new();
    let mut filters_checked = 0u64;
    let mut next = request.start_height;
    let mut covered_through = request.start_height.saturating_sub(1);
    let mut covered_block_hash = request.stop_block_hash;

    while next <= stop_height {
        let batch_request = RangeRequest {
            start_height: next,
            ..request.clone()
        };
        let (batch, batch_charges) = match transport.fetch_range(&batch_request) {
            Ok(result) => result,
            Err(error) => {
                // Charging happens inside the transport; a failed attempt still
                // cost whatever it transferred.
                return Err(error);
            }
        };
        charges.add(batch_charges);
        let checked = check_batch(&batch, &batch_request, chain, limits)?;

        for record in &checked {
            let mapped = map_wallet_scripts(&record.filter, record.block_hash, wallet_scripts);
            let indices = match_mapped(&record.filter, &mapped)?;
            if !indices.is_empty() {
                matches.push(BlockMatch {
                    height: record.height,
                    block_hash: record.block_hash,
                    script_indices: indices,
                });
            }
            filters_checked += 1;
            covered_through = record.height;
            covered_block_hash = record.block_hash;
        }
        next += checked.len() as u64;
    }

    Ok(SyncOutcome {
        matches,
        covered_through,
        covered_block_hash,
        charges,
        filters_checked,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::build_filter::build_filter;
    use crate::envelope::{FilterBatch, FilterRecord, ENVELOPE_VERSION};
    use crate::profile::PROFILE;

    fn hash_at(height: u64) -> BlockHash {
        let mut bytes = [0u8; 32];
        bytes[..8].copy_from_slice(&height.to_le_bytes());
        BlockHash::from_internal_bytes(bytes)
    }

    fn genesis() -> BlockHash {
        BlockHash::from_internal_bytes([0x90u8; 32])
    }

    fn script(tag: u8) -> ScriptBytes {
        ScriptBytes::new(vec![0x76, 0xa9, 0x14, tag])
    }

    struct Fixture {
        chain: ChainMap,
        batches: std::collections::BTreeMap<u64, Vec<u8>>,
    }

    struct MapTransport {
        batches: std::collections::BTreeMap<u64, Vec<u8>>,
        charges: ByteCharges,
    }

    impl FilterTransport for MapTransport {
        fn fetch_range(
            &mut self,
            request: &RangeRequest,
        ) -> Result<(FilterBatch, ByteCharges), FilterError> {
            let bytes = self.batches.get(&request.start_height).ok_or_else(|| {
                FilterError::Response(format!("no batch at {}", request.start_height))
            })?;
            self.charges.requests += 1;
            let charges = ByteCharges {
                received: bytes.len() as u64,
                sent: 0,
                requests: 1,
            };
            Ok((FilterBatch::decode(bytes)?, charges))
        }
    }

    /// Blocks 10..=14; block 12 contains script 7.
    fn fixture() -> Fixture {
        let mut chain = ChainMap::new();
        let mut records = Vec::new();
        for height in 10u64..=14 {
            chain.insert(height, hash_at(height));
            let elements = if height == 12 {
                vec![script(7)]
            } else {
                vec![]
            };
            records.push(FilterRecord {
                height,
                block_hash: hash_at(height),
                filter: build_filter(hash_at(height), &elements).unwrap().0,
            });
        }
        let batch = FilterBatch {
            version: ENVELOPE_VERSION,
            genesis: genesis(),
            profile: PROFILE.to_string(),
            start_height: 10,
            stop_block_hash: hash_at(14),
            records,
        };
        let mut batches = std::collections::BTreeMap::new();
        batches.insert(10u64, batch.encode());
        Fixture { chain, batches }
    }

    fn request() -> RangeRequest {
        RangeRequest {
            genesis: genesis(),
            profile: PROFILE.to_string(),
            start_height: 10,
            stop_block_hash: hash_at(14),
        }
    }

    #[test]
    fn a_matching_script_is_found_at_its_block() {
        let fixture = fixture();
        let mut transport = MapTransport {
            batches: fixture.batches,
            charges: ByteCharges::default(),
        };
        let outcome = sync_range(
            &mut transport,
            &request(),
            &fixture.chain,
            &[script(7)],
            FilterLimits::default(),
        )
        .unwrap();
        assert_eq!(outcome.matches.len(), 1);
        assert_eq!(outcome.matches[0].height, 12);
        assert_eq!(outcome.matches[0].script_indices, vec![0]);
        assert_eq!(outcome.covered_through, 14);
        assert_eq!(outcome.filters_checked, 5);
        assert!(outcome.charges.received > 0);
    }

    #[test]
    fn an_absent_script_yields_coverage_with_no_matches() {
        let fixture = fixture();
        let mut transport = MapTransport {
            batches: fixture.batches,
            charges: ByteCharges::default(),
        };
        let outcome = sync_range(
            &mut transport,
            &request(),
            &fixture.chain,
            &[script(200)],
            FilterLimits::default(),
        )
        .unwrap();
        assert!(outcome.matches.is_empty());
        assert_eq!(outcome.covered_through, 14);
    }

    #[test]
    fn a_missing_middle_record_is_rejected() {
        let fixture = fixture();
        let mut batch = FilterBatch::decode(&fixture.batches[&10]).unwrap();
        batch.records.remove(2);
        let error =
            check_batch(&batch, &request(), &fixture.chain, FilterLimits::default()).unwrap_err();
        assert!(format!("{error}").contains("expected exactly 5"));
    }

    #[test]
    fn a_duplicated_height_is_rejected() {
        let fixture = fixture();
        let mut batch = FilterBatch::decode(&fixture.batches[&10]).unwrap();
        batch.records[3] = batch.records[2].clone();
        let error =
            check_batch(&batch, &request(), &fixture.chain, FilterLimits::default()).unwrap_err();
        assert!(format!("{error}").contains("expected 13"));
    }

    #[test]
    fn excess_records_are_rejected() {
        let fixture = fixture();
        let mut batch = FilterBatch::decode(&fixture.batches[&10]).unwrap();
        let extra = batch.records[4].clone();
        batch.records.push(extra);
        assert!(check_batch(&batch, &request(), &fixture.chain, FilterLimits::default()).is_err());
    }

    #[test]
    fn a_record_on_another_branch_is_rejected() {
        let fixture = fixture();
        let mut batch = FilterBatch::decode(&fixture.batches[&10]).unwrap();
        batch.records[2].block_hash = BlockHash::from_internal_bytes([0xff; 32]);
        let error =
            check_batch(&batch, &request(), &fixture.chain, FilterLimits::default()).unwrap_err();
        assert!(format!("{error}").contains("different branch"));
    }

    #[test]
    fn a_batch_for_another_chain_or_profile_is_rejected() {
        let fixture = fixture();
        let batch = FilterBatch::decode(&fixture.batches[&10]).unwrap();

        let mut wrong_chain = batch.clone();
        wrong_chain.genesis = BlockHash::from_internal_bytes([0x11; 32]);
        assert!(check_batch(
            &wrong_chain,
            &request(),
            &fixture.chain,
            FilterLimits::default()
        )
        .is_err());

        let mut wrong_profile = batch;
        wrong_profile.profile = "something-else".to_string();
        assert!(check_batch(
            &wrong_profile,
            &request(),
            &fixture.chain,
            FilterLimits::default()
        )
        .is_err());
    }

    #[test]
    fn a_terminal_block_the_wallet_has_not_accepted_is_rejected() {
        let fixture = fixture();
        let batch = FilterBatch::decode(&fixture.batches[&10]).unwrap();
        let mut request = request();
        request.stop_block_hash = BlockHash::from_internal_bytes([0xee; 32]);
        let mut batch = batch;
        batch.stop_block_hash = request.stop_block_hash;
        let error =
            check_batch(&batch, &request, &fixture.chain, FilterLimits::default()).unwrap_err();
        assert!(format!("{error}").contains("not on the accepted chain"));
    }

    #[test]
    fn a_cached_old_fork_filter_stops_counting_after_a_rollback() {
        let mut fixture = fixture();
        // The wallet's chain is replaced from height 12 upward.
        fixture.chain.rollback_to(11);
        let batch = FilterBatch::decode(&fixture.batches[&10]).unwrap();
        // The old batch's terminal block is no longer accepted at all.
        let error =
            check_batch(&batch, &request(), &fixture.chain, FilterLimits::default()).unwrap_err();
        assert!(format!("{error}").contains("not on the accepted chain"));
    }

    #[test]
    fn a_malformed_filter_fails_the_whole_batch_rather_than_reading_as_no_match() {
        let fixture = fixture();
        let mut batch = FilterBatch::decode(&fixture.batches[&10]).unwrap();
        batch.records[1].filter = vec![0x05, 0xff];
        let error =
            check_batch(&batch, &request(), &fixture.chain, FilterLimits::default()).unwrap_err();
        assert!(!matches!(error, FilterError::Response(_)));
    }
}
