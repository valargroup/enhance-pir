//! Coordinator and worker runtime for Ironwood Enhance PIR.
//!
//! The public database is indexed directly by Ironwood note position. Physical
//! row shards are an operator detail: clients always submit one global iPIR+SP
//! query and never name a shard.

pub mod coordinator;
pub mod ingest;
pub mod ipir;
pub mod metrics;
pub mod spend;
pub mod store;
pub mod types;
pub mod wire;
pub mod worker;
pub mod zakura;

pub use enhance_pir::{EnhanceGeneration, EnhanceRecord, EnhanceRecordParts, ShardDescriptor};
