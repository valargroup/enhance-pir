//! BIP 158 transparent activity filters for Zcash.
//!
//! This crate implements the `zcash-transparent-basic-v1` application profile:
//! one deterministic filter per accepted block, covering the transparent
//! scripts a block creates and spends, so a wallet can test its own scripts
//! locally without revealing them.
//!
//! What it does not do: it does not prove a filter is complete. Matching a
//! filter against the wallet's accepted block hash binds where the filter
//! claims to be, not what it claims to contain. Advancing coverage on a
//! negative result is only sound under the trusted-indexer assumption recorded
//! in `docs/transparent_pir_contract.md`.

pub mod build_filter;
pub mod client;
pub mod digest;
pub mod envelope;
pub mod error;
pub mod hash;
#[cfg(feature = "client")]
pub mod http;
pub mod matching;
pub mod profile;
pub mod script;
pub mod transport;
pub mod validate;
pub mod wire;

pub use build_filter::{build_filter, element_count, FilterBytes};
pub use client::{
    check_batch, sync_range, AcceptedChain, BlockMatch, ChainMap, CheckedRecord, SyncOutcome,
};
pub use digest::{filter_hash, filter_header, FilterHash, FilterHeader, GENESIS_PREDECESSOR};
pub use envelope::{FilterBatch, FilterRecord, ENVELOPE_VERSION, MAX_RECORDS_PER_BATCH};
pub use error::FilterError;
pub use hash::BlockHash;
pub use matching::{map_wallet_scripts, match_mapped, match_scripts};
pub use profile::{MAINNET_GENESIS_DISPLAY, NETWORK, PROFILE, START_HEIGHT};
pub use script::ScriptBytes;
pub use transport::{ByteCharges, FileTransport, FilterTransport, RangeRequest};
pub use validate::{validate_filter, FilterLimits, ValidatedFilter};
pub use wire::{ChainEntry, FilterServiceHealth, FilterServiceInfo};
