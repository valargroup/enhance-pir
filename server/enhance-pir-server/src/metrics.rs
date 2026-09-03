//! Prometheus metrics for the enhance PIR coordinator.
//!
//! Exposed at `GET /metrics` in the standard text exposition format and
//! scraped locally by the `pir-apm` sidecar (`server/pir-apm`). The registry is
//! process-global so the request middleware and the `query` handler can record
//! observations without threading a handle through router state.
//!
//! ## Privacy
//!
//! Only the fixed, allowlisted client routes are labelled, and the only labels
//! are the endpoint name, HTTP method, and status code. Request bodies, query
//! contents, remote addresses, headers, and arbitrary paths never reach a
//! metric label, so the exposition carries nothing derived from a query.
//!
//! ## Naming
//!
//! Every family is prefixed `enhance_`. Serving-side gauges live under
//! `enhance_snapshot_*` so the sidecar can list them as a group without knowing
//! each name in advance.

use std::sync::OnceLock;
use std::time::Instant;

use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};

use crate::coordinator::CoordinatorPhase;
use crate::types::{DatabaseId, ACTIVATION_HEIGHT, CONFIRMATIONS, SHARDS_PER_WORKER};

struct Metrics {
    registry: Registry,
    http_requests: IntCounterVec,
    http_request_duration: HistogramVec,
    http_request_processing_duration: HistogramVec,
    http_in_flight: IntGaugeVec,
    http_processing_in_flight: IntGaugeVec,
    snapshot_phase_code: IntGauge,
    snapshot_sync_current_height: IntGauge,
    snapshot_sync_target_height: IntGauge,
    snapshot_anchor_height: IntGauge,
    snapshot_generation: IntGauge,
    snapshot_ironwood_tree_size: IntGauge,
    snapshot_retained_generations: IntGauge,
    table_registered: IntGaugeVec,
    table_record_bytes: IntGaugeVec,
    table_records_per_row: IntGaugeVec,
    table_shard_rows: IntGaugeVec,
    table_shard_positions: IntGaugeVec,
    table_shards_per_worker: IntGaugeVec,
    table_pool_workers: IntGaugeVec,
    table_query_slots_available: IntGaugeVec,
    table_positions: IntGaugeVec,
    table_used_rows: IntGaugeVec,
    table_logical_rows: IntGaugeVec,
    table_shards: IntGaugeVec,
    table_sealed_shards: IntGaugeVec,
    worker_up: IntGaugeVec,
    worker_generation: IntGaugeVec,
    worker_table_index: IntGaugeVec,
    worker_table_assigned_shards: IntGaugeVec,
    worker_table_populated_positions: IntGaugeVec,
    worker_table_active_shards: IntGaugeVec,
    worker_index: IntGaugeVec,
    worker_total_memory_bytes: IntGaugeVec,
    worker_available_memory_bytes: IntGaugeVec,
    worker_process_rss_bytes: IntGaugeVec,
    layout_confirmations: IntGauge,
    layout_activation_height: IntGauge,
}

fn worker_gauge(name: &str, help: &str) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), &["worker"]).expect("valid metric")
}

fn table_gauge(name: &str, help: &str) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), &["table"]).expect("valid metric")
}

fn worker_table_gauge(name: &str, help: &str) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), &["worker", "table"]).expect("valid metric")
}

fn gauge(name: &str, help: &str) -> IntGauge {
    IntGauge::new(name, help).expect("valid metric name")
}

fn build_metrics() -> Metrics {
    let registry = Registry::new();

    let http_requests = IntCounterVec::new(
        Opts::new(
            "enhance_http_requests_total",
            "Enhance PIR API requests partitioned by allowlisted endpoint, method, and status.",
        ),
        &["endpoint", "method", "status"],
    )
    .expect("valid metric");
    let http_request_duration = HistogramVec::new(
        HistogramOpts::new(
            "enhance_http_request_duration_seconds",
            "Time from the coordinator receiving request headers until the route produces a \
             response. Includes request body receive time but excludes response transmission.",
        )
        .buckets(vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]),
        &["endpoint"],
    )
    .expect("valid metric");
    let http_request_processing_duration = HistogramVec::new(
        HistogramOpts::new(
            "enhance_http_request_processing_duration_seconds",
            "Time spent answering an allowlisted request after its complete body is available.",
        )
        .buckets(vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]),
        &["endpoint"],
    )
    .expect("valid metric");
    let http_in_flight = IntGaugeVec::new(
        Opts::new(
            "enhance_http_in_flight",
            "Requests received by the coordinator that have not produced a response.",
        ),
        &["endpoint"],
    )
    .expect("valid metric");
    let http_processing_in_flight = IntGaugeVec::new(
        Opts::new(
            "enhance_http_processing_in_flight",
            "Requests being processed after their complete bodies have been received.",
        ),
        &["endpoint"],
    )
    .expect("valid metric");

    let snapshot_phase_code = gauge(
        "enhance_snapshot_phase_code",
        "Coordinator phase: 0 syncing, 1 building, 2 serving, 3 failed.",
    );
    let snapshot_sync_current_height = gauge(
        "enhance_snapshot_sync_current_height",
        "Height reached by the current sync (0 unless syncing).",
    );
    let snapshot_sync_target_height = gauge(
        "enhance_snapshot_sync_target_height",
        "Height the current sync is aiming for (0 unless syncing).",
    );
    let snapshot_anchor_height = gauge(
        "enhance_snapshot_anchor_height",
        "Anchor height of the snapshot currently being served (0 if none).",
    );
    let snapshot_generation = gauge(
        "enhance_snapshot_generation",
        "Generation of the snapshot currently being served (0 if none).",
    );
    let snapshot_ironwood_tree_size = gauge(
        "enhance_snapshot_ironwood_tree_size",
        "Ironwood tree size covered by the served snapshot (0 if none).",
    );
    let snapshot_retained_generations = gauge(
        "enhance_snapshot_retained_generations",
        "Generations still answerable (the newest plus the one before it).",
    );

    // Per-table view. The `table` label is the wire name of a `DatabaseId`,
    // so cardinality is fixed by the enum. Planned tables are exported too,
    // with `registered` 0, so the dashboard can draw the whole design.
    let table_registered = table_gauge(
        "enhance_table_registered",
        "1 if the coordinator serves this table, 0 if it is only planned.",
    );
    let table_record_bytes = table_gauge("enhance_table_record_bytes", "Bytes in one record.");
    let table_records_per_row = table_gauge(
        "enhance_table_records_per_row",
        "Records packed into one PIR row.",
    );
    let table_shard_rows = table_gauge("enhance_table_shard_rows", "PIR rows in one shard.");
    let table_shard_positions = table_gauge(
        "enhance_table_shard_positions",
        "Positions (records) covered by one shard.",
    );
    let table_shards_per_worker = table_gauge(
        "enhance_table_shards_per_worker",
        "Fixed number of shard ids each worker in the table's pool owns.",
    );
    let table_pool_workers = table_gauge(
        "enhance_table_pool_workers",
        "Workers in the table's ordered pool.",
    );
    let table_query_slots_available = table_gauge(
        "enhance_table_query_slots_available",
        "Free concurrent query slots for the table; queries are shed with 503 at 0.",
    );
    let table_positions = table_gauge(
        "enhance_table_positions",
        "Positions published for the table in the newest generation.",
    );
    let table_used_rows = table_gauge(
        "enhance_table_used_rows",
        "Rows holding real records in the newest generation.",
    );
    let table_logical_rows = table_gauge(
        "enhance_table_logical_rows",
        "Public database size in rows (power of two) in the newest generation.",
    );
    let table_shards = table_gauge(
        "enhance_table_shards",
        "Shards published for the table in the newest generation.",
    );
    let table_sealed_shards = table_gauge(
        "enhance_table_sealed_shards",
        "Published shards that are full and will never be rebuilt.",
    );

    // Per-worker fleet view. The `worker` label is the operator-assigned
    // inventory name, so cardinality is bounded by the worker count.
    let worker_up = worker_gauge(
        "enhance_worker_up",
        "1 if the worker answered its health probe on the last scrape, else 0.",
    );
    let worker_generation = worker_gauge(
        "enhance_worker_generation",
        "Generation the worker reports as active (0 if unknown).",
    );
    // Per-worker, per-table ownership: shard id spaces and pool order are
    // per table, so these are only meaningful with both labels.
    let worker_table_index = worker_table_gauge(
        "enhance_worker_table_index",
        "Zero-based position of the worker in this table's pool; fixes which shards it owns.",
    );
    let worker_table_assigned_shards = worker_table_gauge(
        "enhance_worker_table_assigned_shards",
        "Shards of this table the newest generation assigns to the worker.",
    );
    let worker_table_populated_positions = worker_table_gauge(
        "enhance_worker_table_populated_positions",
        "Positions held by the worker's shards of this table.",
    );
    let worker_table_active_shards = worker_table_gauge(
        "enhance_worker_table_active_shards",
        "Shards of this table the worker reports active in its newest generation.",
    );

    let worker_index = worker_gauge(
        "enhance_worker_index",
        "Zero-based position of the worker across all pools, first seen first.",
    );
    let worker_total_memory_bytes = worker_gauge(
        "enhance_worker_total_memory_bytes",
        "Total RAM on the worker host, as reported by its health probe.",
    );
    let worker_available_memory_bytes = worker_gauge(
        "enhance_worker_available_memory_bytes",
        "Available RAM on the worker host, as reported by its health probe.",
    );
    let worker_process_rss_bytes = worker_gauge(
        "enhance_worker_process_rss_bytes",
        "Resident memory of the worker process, as reported by its health probe.",
    );

    // Chain-level constants, exported so the dashboard's explainer never
    // hardcodes a number that lives in `types.rs`.
    let layout_confirmations = gauge(
        "enhance_layout_confirmations",
        "Confirmations a block needs before its actions are ingested.",
    );
    let layout_activation_height = gauge(
        "enhance_layout_activation_height",
        "Height at which Ironwood activated; ingest starts here.",
    );

    for collector in [
        Box::new(http_requests.clone()) as Box<dyn prometheus::core::Collector>,
        Box::new(http_request_duration.clone()),
        Box::new(http_request_processing_duration.clone()),
        Box::new(http_in_flight.clone()),
        Box::new(http_processing_in_flight.clone()),
        Box::new(snapshot_phase_code.clone()),
        Box::new(snapshot_sync_current_height.clone()),
        Box::new(snapshot_sync_target_height.clone()),
        Box::new(snapshot_anchor_height.clone()),
        Box::new(snapshot_generation.clone()),
        Box::new(snapshot_ironwood_tree_size.clone()),
        Box::new(snapshot_retained_generations.clone()),
        Box::new(table_registered.clone()),
        Box::new(table_record_bytes.clone()),
        Box::new(table_records_per_row.clone()),
        Box::new(table_shard_rows.clone()),
        Box::new(table_shard_positions.clone()),
        Box::new(table_shards_per_worker.clone()),
        Box::new(table_pool_workers.clone()),
        Box::new(table_query_slots_available.clone()),
        Box::new(table_positions.clone()),
        Box::new(table_used_rows.clone()),
        Box::new(table_logical_rows.clone()),
        Box::new(table_shards.clone()),
        Box::new(table_sealed_shards.clone()),
        Box::new(worker_up.clone()),
        Box::new(worker_generation.clone()),
        Box::new(worker_table_index.clone()),
        Box::new(worker_table_assigned_shards.clone()),
        Box::new(worker_table_populated_positions.clone()),
        Box::new(worker_table_active_shards.clone()),
        Box::new(worker_index.clone()),
        Box::new(worker_total_memory_bytes.clone()),
        Box::new(worker_available_memory_bytes.clone()),
        Box::new(worker_process_rss_bytes.clone()),
        Box::new(layout_confirmations.clone()),
        Box::new(layout_activation_height.clone()),
    ] {
        registry.register(collector).expect("register collector");
    }
    #[cfg(target_os = "linux")]
    registry
        .register(Box::new(
            prometheus::process_collector::ProcessCollector::for_self(),
        ))
        .expect("register process collector");

    Metrics {
        registry,
        http_requests,
        http_request_duration,
        http_request_processing_duration,
        http_in_flight,
        http_processing_in_flight,
        snapshot_phase_code,
        snapshot_sync_current_height,
        snapshot_sync_target_height,
        snapshot_anchor_height,
        snapshot_generation,
        snapshot_ironwood_tree_size,
        snapshot_retained_generations,
        table_registered,
        table_record_bytes,
        table_records_per_row,
        table_shard_rows,
        table_shard_positions,
        table_shards_per_worker,
        table_pool_workers,
        table_query_slots_available,
        table_positions,
        table_used_rows,
        table_logical_rows,
        table_shards,
        table_sealed_shards,
        worker_up,
        worker_generation,
        worker_table_index,
        worker_table_assigned_shards,
        worker_table_populated_positions,
        worker_table_active_shards,
        worker_index,
        worker_total_memory_bytes,
        worker_available_memory_bytes,
        worker_process_rss_bytes,
        layout_confirmations,
        layout_activation_height,
    }
}

fn metrics() -> &'static Metrics {
    static INSTANCE: OnceLock<Metrics> = OnceLock::new();
    INSTANCE.get_or_init(build_metrics)
}

pub fn query_endpoint(_table: DatabaseId) -> &'static str {
    "query"
}

fn table_endpoint(table: DatabaseId, endpoint: &str) -> Option<&'static str> {
    Some(match (table, endpoint) {
        (DatabaseId::Enhance, "params") => "params",
        (DatabaseId::Enhance, "public-params") => "public_params",
        (DatabaseId::Enhance, "query") => "query",
        _ => return None,
    })
}

/// Fixed client routes that are tracked. Anything else — including `/metrics`
/// and `/ready` themselves — is deliberately untracked so label cardinality is
/// bounded and no caller-controlled path ever becomes a label: every label
/// returned here is a literal from a closed set.
pub fn allowlisted_endpoint(method: &axum::http::Method, path: &str) -> Option<&'static str> {
    use axum::http::Method;
    match (method, path) {
        (&Method::GET, "/v1/health") => return Some("health"),
        (&Method::GET, "/v1/enhance/generation") => return Some("generation"),
        (&Method::GET, "/v1/enhance/params") => return Some("params"),
        (&Method::GET, "/v1/enhance/public-params") => return Some("public_params"),
        (&Method::POST, "/v1/enhance/query") => return Some("query"),
        _ => {}
    }
    let rest = path.strip_prefix("/v1/")?;
    let (table, endpoint) = rest.split_once('/')?;
    let table: DatabaseId = table.parse().ok()?;
    let expected = if endpoint == "query" {
        &Method::POST
    } else {
        &Method::GET
    };
    if method != expected {
        return None;
    }
    table_endpoint(table, endpoint)
}

struct GaugeGuard(IntGauge);

impl GaugeGuard {
    fn new(gauge: IntGauge) -> Self {
        gauge.inc();
        Self(gauge)
    }
}

impl Drop for GaugeGuard {
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// Observes post-upload processing time and keeps the processing concurrency
/// gauge balanced for as long as it is alive.
pub struct ProcessingTimer {
    started: Instant,
    histogram: Histogram,
    _in_flight: GaugeGuard,
}

impl Drop for ProcessingTimer {
    fn drop(&mut self) {
        self.histogram.observe(self.started.elapsed().as_secs_f64());
    }
}

/// Start measuring server-side work for `endpoint` once the complete request
/// body is available. Drop the returned timer when the response is ready.
pub fn start_processing(endpoint: &'static str) -> ProcessingTimer {
    let m = metrics();
    ProcessingTimer {
        started: Instant::now(),
        histogram: m
            .http_request_processing_duration
            .with_label_values(&[endpoint]),
        _in_flight: GaugeGuard::new(m.http_processing_in_flight.with_label_values(&[endpoint])),
    }
}

/// Axum middleware recording the allowlisted client routes.
pub async fn track_request(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let Some(endpoint) = allowlisted_endpoint(request.method(), request.uri().path()) else {
        return next.run(request).await;
    };
    let method = request.method().as_str().to_owned();
    let m = metrics();
    let started = Instant::now();
    let _in_flight = GaugeGuard::new(m.http_in_flight.with_label_values(&[endpoint]));
    let response = next.run(request).await;
    m.http_request_duration
        .with_label_values(&[endpoint])
        .observe(started.elapsed().as_secs_f64());
    m.http_requests
        .with_label_values(&[endpoint, &method, response.status().as_str()])
        .inc();
    response
}

/// Point-in-time view of the coordinator used to populate the snapshot gauges.
/// Built by the coordinator right before each exposition so the ingest path
/// never has to remember to update a gauge.
/// One table as the coordinator sees it at scrape time. Layout fields come
/// from the table's constant `DatabaseLayout`; the rest are zero for a table
/// that is planned but not registered, or not yet published.
#[derive(Clone, Debug)]
pub struct TableObservation {
    pub table: DatabaseId,
    pub registered: bool,
    pub pool_workers: u64,
    pub query_slots_available: u64,
    pub positions: u64,
    pub used_rows: u64,
    pub logical_rows: u64,
    pub shards: u64,
    pub sealed_shards: u64,
}

/// One worker's share of one table.
#[derive(Clone, Debug)]
pub struct WorkerTableObservation {
    pub table: DatabaseId,
    /// Position in this table's pool; the shard ids it owns derive from it.
    pub index: u64,
    pub assigned_shards: u64,
    pub populated_positions: u64,
    pub active_shards: u64,
}

/// What the coordinator knows about one worker at scrape time.
#[derive(Clone, Debug, Default)]
pub struct WorkerObservation {
    pub name: String,
    /// Zero-based position across all pools, first seen first.
    pub index: u64,
    /// `false` when the probe failed or timed out.
    pub up: bool,
    pub generation: u64,
    pub total_memory_bytes: u64,
    pub available_memory_bytes: u64,
    pub process_rss_bytes: u64,
    pub tables: Vec<WorkerTableObservation>,
}

#[derive(Clone, Debug, Default)]
pub struct Observation {
    pub phase: Option<CoordinatorPhase>,
    pub anchor_height: u64,
    pub generation: u64,
    pub ironwood_tree_size: u64,
    pub retained_generations: u64,
    pub tables: Vec<TableObservation>,
    pub worker_details: Vec<WorkerObservation>,
}

fn clamp(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

/// Refresh every `enhance_snapshot_*` gauge from `observation`.
pub fn record_observation(observation: &Observation) {
    let m = metrics();
    let (code, current, target) = match &observation.phase {
        None => (0, 0, 0),
        Some(CoordinatorPhase::Syncing {
            current_height,
            target_height,
        }) => (0, *current_height, *target_height),
        Some(CoordinatorPhase::Building { .. }) => (1, 0, 0),
        Some(CoordinatorPhase::Serving) => (2, 0, 0),
        Some(CoordinatorPhase::Failed { .. }) => (3, 0, 0),
    };
    m.snapshot_phase_code.set(code);
    m.snapshot_sync_current_height.set(clamp(current));
    m.snapshot_sync_target_height.set(clamp(target));
    m.snapshot_anchor_height
        .set(clamp(observation.anchor_height));
    m.snapshot_generation.set(clamp(observation.generation));
    m.snapshot_ironwood_tree_size
        .set(clamp(observation.ironwood_tree_size));
    m.snapshot_retained_generations
        .set(clamp(observation.retained_generations));
    for table in &observation.tables {
        let layout = table.table.layout();
        let label = [table.table.as_str()];
        m.table_registered
            .with_label_values(&label)
            .set(i64::from(table.registered));
        m.table_record_bytes
            .with_label_values(&label)
            .set(clamp(layout.record_bytes as u64));
        m.table_records_per_row
            .with_label_values(&label)
            .set(clamp(layout.records_per_row as u64));
        m.table_shard_rows
            .with_label_values(&label)
            .set(clamp(layout.shard_rows as u64));
        m.table_shard_positions
            .with_label_values(&label)
            .set(clamp(layout.shard_positions() as u64));
        m.table_shards_per_worker
            .with_label_values(&label)
            .set(clamp(SHARDS_PER_WORKER));
        m.table_pool_workers
            .with_label_values(&label)
            .set(clamp(table.pool_workers));
        m.table_query_slots_available
            .with_label_values(&label)
            .set(clamp(table.query_slots_available));
        m.table_positions
            .with_label_values(&label)
            .set(clamp(table.positions));
        m.table_used_rows
            .with_label_values(&label)
            .set(clamp(table.used_rows));
        m.table_logical_rows
            .with_label_values(&label)
            .set(clamp(table.logical_rows));
        m.table_shards
            .with_label_values(&label)
            .set(clamp(table.shards));
        m.table_sealed_shards
            .with_label_values(&label)
            .set(clamp(table.sealed_shards));
    }
    for worker in &observation.worker_details {
        let label = [worker.name.as_str()];
        m.worker_up
            .with_label_values(&label)
            .set(i64::from(worker.up));
        m.worker_generation
            .with_label_values(&label)
            .set(clamp(worker.generation));
        m.worker_index
            .with_label_values(&label)
            .set(clamp(worker.index));
        m.worker_total_memory_bytes
            .with_label_values(&label)
            .set(clamp(worker.total_memory_bytes));
        m.worker_available_memory_bytes
            .with_label_values(&label)
            .set(clamp(worker.available_memory_bytes));
        m.worker_process_rss_bytes
            .with_label_values(&label)
            .set(clamp(worker.process_rss_bytes));
        for share in &worker.tables {
            let label = [worker.name.as_str(), share.table.as_str()];
            m.worker_table_index
                .with_label_values(&label)
                .set(clamp(share.index));
            m.worker_table_assigned_shards
                .with_label_values(&label)
                .set(clamp(share.assigned_shards));
            m.worker_table_populated_positions
                .with_label_values(&label)
                .set(clamp(share.populated_positions));
            m.worker_table_active_shards
                .with_label_values(&label)
                .set(clamp(share.active_shards));
        }
    }
    m.layout_confirmations.set(clamp(CONFIRMATIONS));
    m.layout_activation_height.set(clamp(ACTIVATION_HEIGHT));
}

/// Render the registry as Prometheus text exposition.
pub fn encode() -> (axum::http::StatusCode, String, String) {
    let m = metrics();
    let families = m.registry.gather();
    let encoder = TextEncoder::new();
    let content_type = encoder.format_type().to_string();
    let mut buf = Vec::with_capacity(4096);
    if let Err(error) = encoder.encode(&families, &mut buf) {
        return (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            "text/plain; charset=utf-8".to_string(),
            format!("metrics encode failed: {error}"),
        );
    }
    let body = String::from_utf8(buf).unwrap_or_else(|_| "<invalid utf-8>".to_string());
    (axum::http::StatusCode::OK, content_type, body)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{
        body::Body,
        http::{Method, Request, StatusCode},
        routing::{get, post},
        Router,
    };
    use tower::ServiceExt;

    fn counter(endpoint: &str, method: &str, status: &str) -> u64 {
        metrics()
            .http_requests
            .with_label_values(&[endpoint, method, status])
            .get()
    }

    #[test]
    fn only_fixed_client_routes_are_labelled() {
        assert_eq!(
            allowlisted_endpoint(&Method::GET, "/v1/health"),
            Some("health")
        );
        assert_eq!(
            allowlisted_endpoint(&Method::GET, "/v1/enhance/generation"),
            Some("generation")
        );
        assert_eq!(
            allowlisted_endpoint(&Method::GET, "/v1/enhance/public-params"),
            Some("public_params")
        );
        assert_eq!(
            allowlisted_endpoint(&Method::POST, "/v1/enhance/query"),
            Some("query")
        );
        assert_eq!(
            allowlisted_endpoint(&Method::GET, "/v1/enhance/query"),
            None
        );
        assert_eq!(allowlisted_endpoint(&Method::GET, "/v1/generation"), None);
        assert_eq!(
            allowlisted_endpoint(&Method::GET, "/v1/enhance/secret"),
            None
        );
        assert_eq!(
            allowlisted_endpoint(&Method::GET, "/v1/enhance/../query"),
            None
        );
        assert_eq!(allowlisted_endpoint(&Method::GET, "/metrics"), None);
        assert_eq!(allowlisted_endpoint(&Method::GET, "/ready"), None);
    }

    #[tokio::test]
    async fn middleware_counts_allowlisted_requests_and_balances_in_flight() {
        let app = Router::new()
            .route("/v1/enhance/generation", get(|| async { "ok" }))
            .route(
                "/v1/enhance/query",
                post(|| async {
                    let _timer = start_processing("query");
                    assert_eq!(
                        metrics()
                            .http_processing_in_flight
                            .with_label_values(&["query"])
                            .get(),
                        1
                    );
                    StatusCode::SERVICE_UNAVAILABLE
                }),
            )
            .route("/other", get(|| async { "untracked" }))
            .layer(axum::middleware::from_fn(track_request));

        let before_generation = counter("generation", "GET", "200");
        let before_query = counter("query", "POST", "503");
        for (method, path) in [
            (Method::GET, "/v1/enhance/generation"),
            (Method::POST, "/v1/enhance/query"),
            (Method::GET, "/other"),
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri(path)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert!(response.status().is_success() || response.status() == 503);
        }

        assert_eq!(counter("generation", "GET", "200"), before_generation + 1);
        assert_eq!(counter("query", "POST", "503"), before_query + 1);
        let m = metrics();
        assert_eq!(m.http_in_flight.with_label_values(&["generation"]).get(), 0);
        assert_eq!(m.http_in_flight.with_label_values(&["query"]).get(), 0);
        assert_eq!(
            m.http_processing_in_flight
                .with_label_values(&["query"])
                .get(),
            0
        );
        assert_eq!(
            m.http_request_processing_duration
                .with_label_values(&["query"])
                .get_sample_count(),
            1
        );
        let (status, content_type, body) = encode();
        assert_eq!(status, StatusCode::OK);
        assert!(content_type.starts_with("text/plain"));
        assert!(body.contains("enhance_http_requests_total{endpoint=\"generation\""));
        assert!(!body.contains("/other"));
    }

    #[test]
    fn observation_populates_snapshot_gauges() {
        record_observation(&Observation {
            phase: Some(CoordinatorPhase::Syncing {
                current_height: 10,
                target_height: 20,
            }),
            ..Default::default()
        });
        let m = metrics();
        assert_eq!(m.snapshot_phase_code.get(), 0);
        assert_eq!(m.snapshot_sync_current_height.get(), 10);
        assert_eq!(m.snapshot_sync_target_height.get(), 20);

        let enhance_layout = DatabaseId::Enhance.layout();
        record_observation(&Observation {
            phase: Some(CoordinatorPhase::Serving),
            anchor_height: 3_000_000,
            generation: 3_000_000,
            ironwood_tree_size: u64::MAX,
            retained_generations: 2,
            tables: vec![TableObservation {
                table: DatabaseId::Enhance,
                registered: true,
                pool_workers: 2,
                query_slots_available: 2,
                positions: 138_124,
                used_rows: 17_266,
                logical_rows: 32_768,
                shards: 3,
                sealed_shards: 2,
            }],
            worker_details: vec![
                WorkerObservation {
                    name: "worker-1".into(),
                    index: 0,
                    up: true,
                    generation: 3_000_000,
                    total_memory_bytes: 64 << 30,
                    available_memory_bytes: 60 << 30,
                    process_rss_bytes: 1 << 30,
                    tables: vec![WorkerTableObservation {
                        table: DatabaseId::Enhance,
                        index: 0,
                        assigned_shards: 2,
                        populated_positions: 131_072,
                        active_shards: 2,
                    }],
                },
                WorkerObservation {
                    name: "worker-2".into(),
                    index: 1,
                    up: false,
                    ..Default::default()
                },
            ],
        });
        assert_eq!(m.snapshot_phase_code.get(), 2);
        assert_eq!(m.snapshot_sync_current_height.get(), 0);
        assert_eq!(m.snapshot_anchor_height.get(), 3_000_000);
        assert_eq!(m.snapshot_ironwood_tree_size.get(), i64::MAX);
        assert_eq!(m.snapshot_retained_generations.get(), 2);
        let (_, _, body) = encode();
        assert!(body.contains("enhance_snapshot_phase_code 2"));
        assert!(body.contains("enhance_table_registered{table=\"enhance\"} 1"));
        assert!(body.contains("enhance_table_shards{table=\"enhance\"} 3"));
        assert!(body.contains("enhance_table_sealed_shards{table=\"enhance\"} 2"));
        assert!(body.contains(&format!(
            "enhance_table_shard_positions{{table=\"enhance\"}} {}",
            enhance_layout.shard_positions()
        )));
        assert!(body.contains(&format!(
            "enhance_table_shards_per_worker{{table=\"enhance\"}} {}",
            SHARDS_PER_WORKER
        )));
        assert!(body.contains("enhance_worker_up{worker=\"worker-1\"} 1"));
        assert!(body.contains("enhance_worker_up{worker=\"worker-2\"} 0"));
        assert!(body.contains("enhance_worker_index{worker=\"worker-2\"} 1"));
        assert!(body.contains(
            "enhance_worker_table_assigned_shards{table=\"enhance\",worker=\"worker-1\"} 2"
        ));
        assert!(
            body.contains("enhance_worker_table_index{table=\"enhance\",worker=\"worker-1\"} 0")
        );
        assert!(body.contains(&format!(
            "enhance_worker_total_memory_bytes{{worker=\"worker-1\"}} {}",
            64u64 << 30
        )));
        assert!(body.contains(&format!("enhance_layout_confirmations {}", CONFIRMATIONS)));
        assert!(!body.contains("enhance_layout_shard_positions"));
        assert!(!body.contains("enhance_snapshot_workers"));
    }
}
