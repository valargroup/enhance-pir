//! JSON metadata shapes exchanged with the filter service.
//!
//! These live outside the `client` feature so the service can build its
//! responses from the same definitions a wallet parses, rather than the two
//! sides drifting apart in separate hand-written structs.
//!
//! Hashes appear here in display hex, because these are the human-facing
//! surfaces. Binary serialization uses internal order; see `envelope.rs`.

use serde::{Deserialize, Serialize};

/// Response shape of `GET /v1/filters/info`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FilterServiceInfo {
    /// Genesis block hash in display hex; the chain's identity.
    pub genesis_hash: String,
    pub network: String,
    pub profile: String,
    pub envelope_version: u16,
    /// First height this service publishes a filter for.
    pub start_height: u64,
    /// Highest height with durable coverage, if any.
    pub covered_through: Option<u64>,
    pub covered_block_hash: Option<String>,
    pub max_records_per_batch: u64,
    pub max_filter_bytes: usize,
}

/// Response shape of `GET /v1/health`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct FilterServiceHealth {
    /// `syncing`, `serving` or `failed`.
    pub phase: String,
    pub detail: Option<String>,
    pub covered_through: Option<u64>,
    pub tip_height: Option<u64>,
    pub filters_stored: u64,
}

/// One entry of `GET /v1/filters/chain`.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ChainEntry {
    pub height: u64,
    /// Display hex, as a human-facing JSON field.
    pub block_hash: String,
}
