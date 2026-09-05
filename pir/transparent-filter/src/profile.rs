//! The `zcash-transparent-basic-v1` application profile.
//!
//! This is a proposed application profile that reuses BIP 158's basic encoding
//! and analogous transparent-script inclusion rules. It is not an assertion of
//! an existing Zcash network standard, and it implies no support for Bitcoin
//! peer-service messages or signalling.

/// Profile identifier carried in application metadata and cache keys.
pub const PROFILE: &str = "zcash-transparent-basic-v1";

/// Golomb-Rice parameter. Fixed by the profile; not configurable.
pub const P: u8 = 19;

/// False-positive range parameter. Fixed by the profile; not configurable.
pub const M: u64 = 784_931;

/// Network identifier carried alongside the genesis hash.
pub const NETWORK: &str = "main";

/// Zcash mainnet genesis block hash in RPC display order.
///
/// Chain identity is the genesis hash, not the network name: the name alone
/// would let a filter built on one chain be accepted on another that happens to
/// share it.
pub const MAINNET_GENESIS_DISPLAY: &str =
    "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08";

/// First height this profile publishes filters for.
///
/// Coverage begins at Ironwood activation rather than genesis. A wallet whose
/// birthday precedes this height is not served by this deployment; that is a
/// deployment limitation, not a property of the encoding.
pub const START_HEIGHT: u64 = 3_428_143;
