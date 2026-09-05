//! Shared service state and the HTTP surface.

use crate::store::{FilterStore, StoreError};
use axum::extract::{Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::Router;
use std::sync::Arc;
use tokio::sync::RwLock;
use transparent_filter::envelope::{FilterBatch, FilterRecord, ENVELOPE_VERSION};
use transparent_filter::wire::{ChainEntry, FilterServiceHealth, FilterServiceInfo};
use transparent_filter::{BlockHash, FilterLimits, MAX_RECORDS_PER_BATCH};

/// Where ingestion has got to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Phase {
    Syncing {
        current_height: Option<u64>,
        target_height: u64,
    },
    Serving,
    Failed {
        reason: String,
    },
}

impl Phase {
    pub fn name(&self) -> &'static str {
        match self {
            Phase::Syncing { .. } => "syncing",
            Phase::Serving => "serving",
            Phase::Failed { .. } => "failed",
        }
    }
}

pub struct Inner {
    pub store: FilterStore,
    pub phase: Phase,
    pub tip_height: Option<u64>,
}

#[derive(Clone)]
pub struct ServiceState {
    inner: Arc<RwLock<Inner>>,
    metrics: Arc<crate::metrics::Metrics>,
}

impl ServiceState {
    pub fn new(store: FilterStore) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Inner {
                store,
                phase: Phase::Syncing {
                    current_height: None,
                    target_height: 0,
                },
                tip_height: None,
            })),
            metrics: Arc::new(crate::metrics::Metrics::new()),
        }
    }

    pub fn inner(&self) -> &Arc<RwLock<Inner>> {
        &self.inner
    }
    pub fn metrics(&self) -> &Arc<crate::metrics::Metrics> {
        &self.metrics
    }

    pub async fn set_phase(&self, phase: Phase) {
        self.inner.write().await.phase = phase;
    }

    pub async fn set_tip(&self, tip: u64) {
        self.inner.write().await.tip_height = Some(tip);
    }
}

#[derive(serde::Deserialize)]
pub struct RangeParams {
    start_height: u64,
    /// Display hex, as a caller reading a block explorer would supply it.
    stop_block_hash: String,
}

#[derive(serde::Deserialize)]
pub struct ChainParams {
    start_height: u64,
    count: u64,
}

fn json(status: StatusCode, body: serde_json::Value) -> Response {
    (
        status,
        [("content-type", "application/json")],
        body.to_string(),
    )
        .into_response()
}

fn bad_request(message: impl std::fmt::Display) -> Response {
    json(
        StatusCode::BAD_REQUEST,
        serde_json::json!({ "error": message.to_string() }),
    )
}

pub fn router(state: ServiceState) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/filters/info", get(info))
        .route("/v1/filters/chain", get(chain))
        .route("/v1/filters/range", get(range))
        .route("/metrics", get(metrics))
        .route("/ready", get(ready))
        .with_state(state)
}

async fn health(State(state): State<ServiceState>) -> Response {
    let inner = state.inner.read().await;
    let body = FilterServiceHealth {
        phase: inner.phase.name().to_string(),
        detail: match &inner.phase {
            Phase::Failed { reason } => Some(reason.clone()),
            Phase::Syncing {
                current_height,
                target_height,
            } => Some(format!(
                "at {} of {target_height}",
                current_height.map_or("nothing".to_string(), |h| h.to_string())
            )),
            Phase::Serving => None,
        },
        covered_through: inner.store.covered_through(),
        tip_height: inner.tip_height,
        filters_stored: inner.store.filters_stored(),
    };
    json(
        StatusCode::OK,
        serde_json::to_value(body).expect("health json"),
    )
}

async fn info(State(state): State<ServiceState>) -> Response {
    let inner = state.inner.read().await;
    let body = FilterServiceInfo {
        genesis_hash: inner.store.genesis_hash().to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: inner.store.profile().to_string(),
        envelope_version: ENVELOPE_VERSION,
        start_height: inner.store.start_height(),
        covered_through: inner.store.covered_through(),
        covered_block_hash: inner
            .store
            .covered_through()
            .and_then(|height| inner.store.block_hash_at(height))
            .map(|hash| hash.to_display_hex()),
        max_records_per_batch: MAX_RECORDS_PER_BATCH,
        max_filter_bytes: FilterLimits::default().max_bytes,
    };
    json(
        StatusCode::OK,
        serde_json::to_value(body).expect("info json"),
    )
}

/// Height-to-hash for a range.
///
/// A wallet must not take its accepted chain from the same service that serves
/// the filters; this exists so a client with no chain of its own can be
/// exercised end to end, and so an operator can check what the service believes
/// it has covered.
async fn chain(State(state): State<ServiceState>, Query(params): Query<ChainParams>) -> Response {
    let inner = state.inner.read().await;
    if params.count == 0 || params.count > 10_000 {
        return bad_request("count must be between 1 and 10000");
    }
    let mut entries = Vec::new();
    for height in params.start_height..params.start_height.saturating_add(params.count) {
        match inner.store.block_hash_at(height) {
            Some(hash) => entries.push(ChainEntry {
                height,
                block_hash: hash.to_display_hex(),
            }),
            // Coverage ends here; return the prefix rather than an error.
            None => break,
        }
    }
    json(
        StatusCode::OK,
        serde_json::to_value(entries).expect("chain json"),
    )
}

/// One bounded batch of filters.
///
/// The request carries chain range information only. There is no parameter in
/// which a script, address, outpoint or match could be expressed.
async fn range(State(state): State<ServiceState>, Query(params): Query<RangeParams>) -> Response {
    let inner = state.inner.read().await;
    let stop_block_hash = match BlockHash::from_display_hex(&params.stop_block_hash) {
        Ok(hash) => hash,
        Err(error) => return bad_request(error),
    };
    let Some(stop_height) = inner.store.height_of(stop_block_hash) else {
        return bad_request("stop_block_hash is not a covered block on the current branch");
    };
    if params.start_height < inner.store.start_height() {
        return bad_request(format!(
            "start_height is below the service's start height {}",
            inner.store.start_height()
        ));
    }
    if params.start_height > stop_height {
        return bad_request("start_height is above the stop block's height");
    }

    let count = (stop_height - params.start_height + 1).min(MAX_RECORDS_PER_BATCH);
    let mut records = Vec::with_capacity(count as usize);
    for height in params.start_height..params.start_height + count {
        let Some(hash) = inner.store.block_hash_at(height) else {
            // Coverage changed under a concurrent rollback; a short batch would
            // fail the client's count check anyway, so say so plainly.
            return bad_request("coverage changed while the batch was being assembled");
        };
        match inner.store.filter_by_hash(hash) {
            Ok(Some(stored)) => records.push(FilterRecord {
                height,
                block_hash: hash,
                filter: stored.bytes,
            }),
            Ok(None) => {
                return json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    serde_json::json!({"error": format!("no stored filter at height {height}")}),
                )
            }
            Err(error) => {
                return json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    serde_json::json!({"error": error.to_string()}),
                )
            }
        }
    }

    let batch = FilterBatch {
        version: ENVELOPE_VERSION,
        genesis: match BlockHash::from_display_hex(inner.store.genesis_hash()) {
            Ok(hash) => hash,
            Err(error) => {
                return json(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    serde_json::json!({"error": error.to_string()}),
                )
            }
        },
        profile: inner.store.profile().to_string(),
        start_height: params.start_height,
        stop_block_hash,
        records,
    };
    let bytes = batch.encode();
    state.metrics.observe_range(count, bytes.len() as u64);
    (
        StatusCode::OK,
        [("content-type", "application/octet-stream")],
        bytes,
    )
        .into_response()
}

async fn metrics(State(state): State<ServiceState>) -> Response {
    let inner = state.inner.read().await;
    state.metrics.update(&inner);
    match state.metrics.render() {
        Ok(text) => (StatusCode::OK, [("content-type", "text/plain")], text).into_response(),
        Err(error) => json(
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error": error}),
        ),
    }
}

/// Readiness. Serving a stale-but-valid range is fine, so readiness keys on
/// having any durable coverage, not on being caught up to the tip: a service
/// that flips to not-ready on every block would be useless behind a proxy.
async fn ready(State(state): State<ServiceState>) -> Response {
    let inner = state.inner.read().await;
    match (&inner.phase, inner.store.covered_through()) {
        (Phase::Failed { reason }, _) => json(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"ready": false, "reason": reason}),
        ),
        (_, None) => json(
            StatusCode::SERVICE_UNAVAILABLE,
            serde_json::json!({"ready": false, "reason": "no coverage yet"}),
        ),
        (_, Some(height)) => json(
            StatusCode::OK,
            serde_json::json!({"ready": true, "covered_through": height}),
        ),
    }
}

impl From<StoreError> for Response {
    fn from(error: StoreError) -> Self {
        json(
            StatusCode::INTERNAL_SERVER_ERROR,
            serde_json::json!({"error": error.to_string()}),
        )
    }
}
