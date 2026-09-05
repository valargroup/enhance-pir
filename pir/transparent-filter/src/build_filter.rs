//! Deterministic per-block filter construction.

use crate::error::FilterError;
use crate::hash::BlockHash;
use crate::profile::{M, P};
use crate::script::ScriptBytes;
use bitcoin::bip158::GcsFilterWriter;
use std::collections::BTreeSet;

/// Serialized BIP 158 filter bytes for exactly one block.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilterBytes(pub Vec<u8>);

impl FilterBytes {
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }
    pub fn len(&self) -> usize {
        self.0.len()
    }
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl AsRef<[u8]> for FilterBytes {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

/// Builds the filter for one block from its complete element set.
///
/// One filter represents exactly one accepted block. Delivery objects may
/// bundle filters, but must never merge them into a single differently keyed
/// filter, because the SipHash keys are derived from this block's hash.
///
/// Elements are deduplicated by raw script bytes before encoding. Deduplication
/// is on the bytes only: two distinct scripts whose hashes collide are two
/// elements, and collapsing them would drop coverage.
///
/// Callers are responsible for supplying a *complete* element set. This
/// function cannot tell an intentionally empty block from one whose previous
/// outputs failed to resolve, so a caller that cannot resolve a previous output
/// must fail rather than pass a short list.
pub fn build_filter(
    block_hash: BlockHash,
    elements: &[ScriptBytes],
) -> Result<FilterBytes, FilterError> {
    let (k0, k1) = block_hash.filter_keys();
    // Filter membership is a set; sorting and deduplicating here (rather than
    // relying on the writer's internal set) keeps the element count we report
    // and the count the encoder writes the same number.
    let deduplicated: BTreeSet<&[u8]> = elements
        .iter()
        .filter(|script| script.is_filter_element())
        .map(|script| script.as_slice())
        .collect();

    let mut bytes = Vec::new();
    let mut writer = GcsFilterWriter::new(&mut bytes, k0, k1, M, P);
    for element in &deduplicated {
        writer.add_element(element);
    }
    writer
        .finish()
        .map_err(|error| FilterError::Encoding(error.to_string()))?;
    Ok(FilterBytes(bytes))
}

/// The number of elements `build_filter` would encode for this input.
pub fn element_count(elements: &[ScriptBytes]) -> usize {
    elements
        .iter()
        .filter(|script| script.is_filter_element())
        .map(|script| script.as_slice())
        .collect::<BTreeSet<_>>()
        .len()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash() -> BlockHash {
        BlockHash::from_internal_bytes([7u8; 32])
    }

    #[test]
    fn the_empty_filter_is_exactly_one_zero_byte() {
        assert_eq!(build_filter(hash(), &[]).unwrap().as_slice(), &[0x00]);
    }

    #[test]
    fn scripts_excluded_from_the_element_set_produce_the_empty_filter() {
        let excluded = vec![
            ScriptBytes::new(vec![]),
            ScriptBytes::new(vec![0x6a]),
            ScriptBytes::new(vec![0x6a, 0xff, 0xff]),
        ];
        assert_eq!(build_filter(hash(), &excluded).unwrap().as_slice(), &[0x00]);
        assert_eq!(element_count(&excluded), 0);
    }

    #[test]
    fn duplicate_and_reordered_elements_encode_identically() {
        let a = ScriptBytes::new(vec![0x76, 0xa9, 0x01]);
        let b = ScriptBytes::new(vec![0x51, 0x52]);
        let one = build_filter(hash(), &[a.clone(), b.clone()]).unwrap();
        let reordered = build_filter(hash(), &[b.clone(), a.clone()]).unwrap();
        let duplicated =
            build_filter(hash(), &[b.clone(), a.clone(), a.clone(), b.clone()]).unwrap();
        assert_eq!(one, reordered);
        assert_eq!(one, duplicated);
        assert_eq!(element_count(&[a, b]), 2);
    }

    #[test]
    fn a_different_block_hash_produces_different_bytes_for_the_same_elements() {
        let elements = vec![ScriptBytes::new(vec![0x76, 0xa9, 0x14, 0x01])];
        let one = build_filter(hash(), &elements).unwrap();
        let other = build_filter(BlockHash::from_internal_bytes([9u8; 32]), &elements).unwrap();
        assert_ne!(one, other);
    }
}
