//! Client and protocol types for privately enhancing Ironwood compact actions.

pub mod client;
pub mod types;

pub use types::{
    group_index_for_shard, EnhanceGeneration, EnhanceRecord, EnhanceRecordParts, EnhanceSession,
    InvalidEnhanceRecordFlags, ShardDescriptor, ACTIVATION_HEIGHT, CONFIRMATIONS,
    ENHANCE_SETUP_SEED, FLAG_HAS_TRANSPARENT_BUNDLE, ITEM_SIZE_BITS, KNOWN_FLAGS, NETWORK, POOL,
    PROTOCOL_REVISION, RECORDS_PER_ROW, RECORD_BYTES, RECORD_FLAGS_OFFSET, ROW_BYTES,
    SCHEMA_VERSION, SHARDS_PER_GROUP, SHARDS_PER_WORKER, SHARD_POSITIONS, SHARD_ROWS,
};
