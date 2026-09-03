//! Standalone Ironwood memo PIR proof of concept.
//!
//! The public database is indexed directly by Ironwood note position. Physical
//! row shards are an operator detail: clients always submit one global iPIR+SP
//! query and never name a shard.

pub mod client;
pub mod coordinator;
pub mod ipir;
pub mod metrics;
pub mod store;
pub mod types;
pub mod wire;
pub mod worker;
pub mod zakura;

pub use types::{
    ActionRecord, ActionRecordParts, Coverage, DatabaseId, DatabaseLayout, GenerationManifest,
    MemoSnapshotMetadata, ShardDescriptor, TableManifest, RECORDS_PER_ROW, RECORD_BYTES, ROW_BYTES,
    SHARD_POSITIONS, SHARD_ROWS,
};
