//! Prometheus metrics, prefixed `transparent_filter_`.
//!
//! Loopback-only, like the sibling coordinator's: the reverse proxy returns 404
//! for `/metrics` publicly.

use crate::service::{Inner, Phase};
use prometheus::{IntCounter, IntGauge, Registry};

pub struct Metrics {
    registry: Registry,
    covered_through: IntGauge,
    tip_height: IntGauge,
    filters_stored: IntGauge,
    phase: IntGauge,
    blocks_ingested: IntCounter,
    prevout_rpc_lookups: IntCounter,
    prevout_cache_hits: IntCounter,
    rollbacks: IntCounter,
    range_requests: IntCounter,
    range_records: IntCounter,
    range_bytes: IntCounter,
}

fn gauge(registry: &Registry, name: &str, help: &str) -> IntGauge {
    let gauge = IntGauge::new(name, help).expect("valid gauge");
    registry
        .register(Box::new(gauge.clone()))
        .expect("register gauge");
    gauge
}

fn counter(registry: &Registry, name: &str, help: &str) -> IntCounter {
    let counter = IntCounter::new(name, help).expect("valid counter");
    registry
        .register(Box::new(counter.clone()))
        .expect("register counter");
    counter
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();
        Self {
            covered_through: gauge(
                &registry,
                "transparent_filter_covered_through_height",
                "Highest height with durable filter coverage",
            ),
            tip_height: gauge(
                &registry,
                "transparent_filter_tip_height",
                "Best-chain tip height reported by Zakura",
            ),
            filters_stored: gauge(
                &registry,
                "transparent_filter_filters_stored",
                "Filters held in the store, including orphaned branches",
            ),
            phase: gauge(
                &registry,
                "transparent_filter_phase",
                "Ingest phase: 0 syncing, 1 serving, 2 failed",
            ),
            blocks_ingested: counter(
                &registry,
                "transparent_filter_blocks_ingested_total",
                "Blocks whose filter was built and appended",
            ),
            prevout_rpc_lookups: counter(
                &registry,
                "transparent_filter_prevout_rpc_lookups_total",
                "Previous-output lookups that required an RPC call",
            ),
            prevout_cache_hits: counter(
                &registry,
                "transparent_filter_prevout_cache_hits_total",
                "Previous-output lookups answered from the output cache",
            ),
            rollbacks: counter(
                &registry,
                "transparent_filter_rollbacks_total",
                "Blocks rolled back after a best-chain change",
            ),
            range_requests: counter(
                &registry,
                "transparent_filter_range_requests_total",
                "Range requests answered",
            ),
            range_records: counter(
                &registry,
                "transparent_filter_range_records_total",
                "Filter records delivered",
            ),
            range_bytes: counter(
                &registry,
                "transparent_filter_range_bytes_total",
                "Envelope bytes delivered",
            ),
            registry,
        }
    }

    pub fn observe_block(&self, rpc_lookups: u64, cache_hits: u64) {
        self.blocks_ingested.inc();
        self.prevout_rpc_lookups.inc_by(rpc_lookups);
        self.prevout_cache_hits.inc_by(cache_hits);
    }

    pub fn observe_rollback(&self, blocks: u64) {
        self.rollbacks.inc_by(blocks);
    }

    pub fn observe_range(&self, records: u64, bytes: u64) {
        self.range_requests.inc();
        self.range_records.inc_by(records);
        self.range_bytes.inc_by(bytes);
    }

    pub fn update(&self, inner: &Inner) {
        self.covered_through
            .set(inner.store.covered_through().unwrap_or(0) as i64);
        self.tip_height.set(inner.tip_height.unwrap_or(0) as i64);
        self.filters_stored.set(inner.store.filters_stored() as i64);
        self.phase.set(match inner.phase {
            Phase::Syncing { .. } => 0,
            Phase::Serving => 1,
            Phase::Failed { .. } => 2,
        });
    }

    pub fn render(&self) -> Result<String, String> {
        use prometheus::Encoder;
        let mut buffer = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&self.registry.gather(), &mut buffer)
            .map_err(|error| error.to_string())?;
        String::from_utf8(buffer).map_err(|error| error.to_string())
    }
}
