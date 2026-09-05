//! Block hashes, in the two byte orders that matter, kept apart by type.
//!
//! Zcash RPC and block explorers display a block hash as the hex of its
//! *reversed* serialized bytes. The BIP 158 SipHash keys are derived from the
//! serialized ("internal") order. Confusing the two produces a filter that is
//! self-consistent and wrong, which no round-trip test through a single
//! implementation would catch, so the conversion is named and tested against
//! pinned vectors rather than being spelled inline at each call site.

use crate::error::FilterError;

/// A block hash in canonical internal (serialized, little-endian) byte order.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BlockHash(pub [u8; 32]);

impl BlockHash {
    /// Wraps bytes that are already in internal order.
    pub fn from_internal_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Parses RPC/display hex, reversing into internal order.
    pub fn from_display_hex(text: &str) -> Result<Self, FilterError> {
        let mut bytes = <[u8; 32]>::try_from(
            hex::decode(text)
                .map_err(|_| FilterError::BlockHash("block hash is not hexadecimal".into()))?
                .as_slice(),
        )
        .map_err(|_| FilterError::BlockHash("block hash is not 32 bytes".into()))?;
        bytes.reverse();
        Ok(Self(bytes))
    }

    /// Renders RPC/display hex. Never use this in binary serialization.
    pub fn to_display_hex(self) -> String {
        let mut bytes = self.0;
        bytes.reverse();
        hex::encode(bytes)
    }

    pub fn internal_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// The two little-endian 64-bit SipHash-2-4 keys, taken from the first
    /// sixteen bytes of the internal representation.
    pub fn filter_keys(&self) -> (u64, u64) {
        let k0 = u64::from_le_bytes(self.0[0..8].try_into().expect("8 bytes"));
        let k1 = u64::from_le_bytes(self.0[8..16].try_into().expect("8 bytes"));
        (k0, k1)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::profile::MAINNET_GENESIS_DISPLAY;

    #[test]
    fn display_hex_round_trips_through_internal_order() {
        let hash = BlockHash::from_display_hex(MAINNET_GENESIS_DISPLAY).unwrap();
        assert_eq!(hash.to_display_hex(), MAINNET_GENESIS_DISPLAY);
    }

    #[test]
    fn internal_order_is_the_reverse_of_display_order() {
        let hash = BlockHash::from_display_hex(MAINNET_GENESIS_DISPLAY).unwrap();
        // Display order ends in ...dce08, so internal order begins 08 ce 03 97.
        assert_eq!(&hash.internal_bytes()[..4], &[0x08, 0xce, 0x3d, 0x97]);
        // And the leading display zeros land at the end of internal order.
        assert_eq!(&hash.internal_bytes()[28..], &[0xe8, 0x0f, 0x04, 0x00]);
    }

    #[test]
    fn keys_come_from_the_first_sixteen_internal_bytes() {
        let mut bytes = [0u8; 32];
        for (index, byte) in bytes.iter_mut().enumerate() {
            *byte = index as u8;
        }
        let (k0, k1) = BlockHash::from_internal_bytes(bytes).filter_keys();
        assert_eq!(k0, 0x0706_0504_0302_0100);
        assert_eq!(k1, 0x0f0e_0d0c_0b0a_0908);
    }

    #[test]
    fn malformed_display_hashes_are_rejected() {
        assert!(BlockHash::from_display_hex("nothex").is_err());
        assert!(BlockHash::from_display_hex("00ff").is_err());
        assert!(BlockHash::from_display_hex(&"ab".repeat(33)).is_err());
    }
}
