//! Client and protocol types for privately enhancing Ironwood compact actions.

pub mod client;
pub mod types;

pub use types::{
    group_index_for_shard, EnhanceGeneration, EnhanceRecord, EnhanceRecordParts, EnhanceSession,
    ShardDescriptor, ACTIVATION_HEIGHT, CONFIRMATIONS, ENHANCE_SETUP_SEED, ITEM_SIZE_BITS, NETWORK,
    POOL, PROTOCOL_REVISION, RECORDS_PER_ROW, RECORD_BYTES, ROW_BYTES, SCHEMA_VERSION,
    SHARDS_PER_GROUP, SHARDS_PER_WORKER, SHARD_POSITIONS, SHARD_ROWS,
};
