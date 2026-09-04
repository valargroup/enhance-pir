//! Client and protocol types for private transparent-outpoint spend lookup.

pub mod client;
pub mod types;

pub use client::{ClientError, TransparentSpendPirClient};
pub use types::{
    bucket_count, bucket_for_outpoint, scan_bucket, ShardDescriptor, SpendLookup,
    TransparentSpendEntry, TransparentSpendGeneration, TransparentSpendSession,
    TransparentSpendTableSession, BUCKET_CAPACITY, COLD_SETUP_SEED, ENTRY_BYTES, ITEM_SIZE_BITS,
    NETWORK, PROTOCOL_REVISION, ROW_BYTES, SCHEMA_VERSION, SHARD_ROWS, WARM_BLOCKS,
    WARM_SETUP_SEED,
};
