//! Prometheus metrics for the memo PIR coordinator.
//!
//! Exposed at `GET /metrics` in the standard text exposition format and
//! scraped locally by the `pir-apm` sidecar (`deploy/pir-apm`). The registry is
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
//! Every family is prefixed `memo_`. Serving-side gauges live under
//! `memo_snapshot_*` so the sidecar can list them as a group without knowing
//! each name in advance.

use std::sync::OnceLock;
use std::time::Instant;

use prometheus::{
    Encoder, Histogram, HistogramOpts, HistogramVec, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry, TextEncoder,
};

use crate::coordinator::CoordinatorPhase;

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
    snapshot_used_rows: IntGauge,
    snapshot_shards: IntGauge,
    snapshot_workers: IntGauge,
    snapshot_query_slots_available: IntGauge,
    worker_up: IntGaugeVec,
    worker_generation: IntGaugeVec,
    worker_active_shards: IntGaugeVec,
    worker_assigned_shards: IntGaugeVec,
    worker_populated_positions: IntGaugeVec,
}

fn worker_gauge(name: &str, help: &str) -> IntGaugeVec {
    IntGaugeVec::new(Opts::new(name, help), &["worker"]).expect("valid metric")
}

fn gauge(name: &str, help: &str) -> IntGauge {
    IntGauge::new(name, help).expect("valid metric name")
}

fn build_metrics() -> Metrics {
    let registry = Registry::new();

    let http_requests = IntCounterVec::new(
        Opts::new(
            "memo_http_requests_total",
            "Memo PIR API requests partitioned by allowlisted endpoint, method, and status.",
        ),
        &["endpoint", "method", "status"],
    )
    .expect("valid metric");
    let http_request_duration = HistogramVec::new(
        HistogramOpts::new(
            "memo_http_request_duration_seconds",
            "Time from the coordinator receiving request headers until the route produces a \
             response. Includes request body receive time but excludes response transmission.",
        )
        .buckets(vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]),
        &["endpoint"],
    )
    .expect("valid metric");
    let http_request_processing_duration = HistogramVec::new(
        HistogramOpts::new(
            "memo_http_request_processing_duration_seconds",
            "Time spent answering an allowlisted request after its complete body is available.",
        )
        .buckets(vec![0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 5.0, 10.0, 30.0]),
        &["endpoint"],
    )
    .expect("valid metric");
    let http_in_flight = IntGaugeVec::new(
        Opts::new(
            "memo_http_in_flight",
            "Requests received by the coordinator that have not produced a response.",
        ),
        &["endpoint"],
    )
    .expect("valid metric");
    let http_processing_in_flight = IntGaugeVec::new(
        Opts::new(
            "memo_http_processing_in_flight",
            "Requests being processed after their complete bodies have been received.",
        ),
        &["endpoint"],
    )
    .expect("valid metric");

    let snapshot_phase_code = gauge(
        "memo_snapshot_phase_code",
        "Coordinator phase: 0 syncing, 1 building, 2 serving, 3 failed.",
    );
    let snapshot_sync_current_height = gauge(
        "memo_snapshot_sync_current_height",
        "Height reached by the current sync (0 unless syncing).",
    );
    let snapshot_sync_target_height = gauge(
        "memo_snapshot_sync_target_height",
        "Height the current sync is aiming for (0 unless syncing).",
    );
    let snapshot_anchor_height = gauge(
        "memo_snapshot_anchor_height",
        "Anchor height of the snapshot currently being served (0 if none).",
    );
    let snapshot_generation = gauge(
        "memo_snapshot_generation",
        "Generation of the snapshot currently being served (0 if none).",
    );
    let snapshot_ironwood_tree_size = gauge(
        "memo_snapshot_ironwood_tree_size",
        "Ironwood tree size covered by the served snapshot (0 if none).",
    );
    let snapshot_used_rows = gauge(
        "memo_snapshot_used_rows",
        "Database rows holding real records in the served snapshot (0 if none).",
    );
    let snapshot_shards = gauge(
        "memo_snapshot_shards",
        "Shards in the served snapshot (0 if none).",
    );
    let snapshot_workers = gauge(
        "memo_snapshot_workers",
        "Workers configured on this coordinator.",
    );
    let snapshot_query_slots_available = gauge(
        "memo_snapshot_query_slots_available",
        "Free concurrent query slots; queries are shed with 503 when this reaches 0.",
    );

    // Per-worker fleet view. The `worker` label is the operator-assigned
    // inventory name, so cardinality is bounded by the worker count.
    let worker_up = worker_gauge(
        "memo_worker_up",
        "1 if the worker answered its health probe on the last scrape, else 0.",
    );
    let worker_generation = worker_gauge(
        "memo_worker_generation",
        "Generation the worker reports as active (0 if unknown).",
    );
    let worker_active_shards = worker_gauge(
        "memo_worker_active_shards",
        "Shards the worker reports as active in its current generation.",
    );
    let worker_assigned_shards = worker_gauge(
        "memo_worker_assigned_shards",
        "Shards the served snapshot assigns to this worker.",
    );
    let worker_populated_positions = worker_gauge(
        "memo_worker_populated_positions",
        "Ironwood positions held by the shards assigned to this worker.",
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
        Box::new(snapshot_used_rows.clone()),
        Box::new(snapshot_shards.clone()),
        Box::new(snapshot_workers.clone()),
        Box::new(snapshot_query_slots_available.clone()),
        Box::new(worker_up.clone()),
        Box::new(worker_generation.clone()),
        Box::new(worker_active_shards.clone()),
        Box::new(worker_assigned_shards.clone()),
        Box::new(worker_populated_positions.clone()),
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
        snapshot_used_rows,
        snapshot_shards,
        snapshot_workers,
        snapshot_query_slots_available,
        worker_up,
        worker_generation,
        worker_active_shards,
        worker_assigned_shards,
        worker_populated_positions,
    }
}

fn metrics() -> &'static Metrics {
    static INSTANCE: OnceLock<Metrics> = OnceLock::new();
    INSTANCE.get_or_init(build_metrics)
}

/// Fixed client routes that are tracked. Anything else — including `/metrics`
/// and `/ready` themselves — is deliberately untracked so label cardinality is
/// bounded and no caller-controlled path ever becomes a label.
pub fn allowlisted_endpoint(method: &axum::http::Method, path: &str) -> Option<&'static str> {
    match (method, path) {
        (&axum::http::Method::GET, "/memo/health") => Some("health"),
        (&axum::http::Method::GET, "/memo/metadata") => Some("metadata"),
        (&axum::http::Method::GET, "/memo/params") => Some("params"),
        (&axum::http::Method::GET, "/memo/public-params") => Some("public_params"),
        (&axum::http::Method::POST, "/memo/query") => Some("query"),
        _ => None,
    }
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
/// What the coordinator knows about one worker at scrape time.
#[derive(Clone, Debug, Default)]
pub struct WorkerObservation {
    pub name: String,
    /// `None` when the probe failed or timed out.
    pub up: bool,
    pub generation: u64,
    pub active_shards: u64,
    pub assigned_shards: u64,
    pub populated_positions: u64,
}

#[derive(Clone, Debug, Default)]
pub struct Observation {
    pub phase: Option<CoordinatorPhase>,
    pub anchor_height: u64,
    pub generation: u64,
    pub ironwood_tree_size: u64,
    pub used_rows: u64,
    pub shards: u64,
    pub workers: u64,
    pub query_slots_available: u64,
    pub worker_details: Vec<WorkerObservation>,
}

fn clamp(value: u64) -> i64 {
    value.min(i64::MAX as u64) as i64
}

/// Refresh every `memo_snapshot_*` gauge from `observation`.
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
    m.snapshot_used_rows.set(clamp(observation.used_rows));
    m.snapshot_shards.set(clamp(observation.shards));
    m.snapshot_workers.set(clamp(observation.workers));
    m.snapshot_query_slots_available
        .set(clamp(observation.query_slots_available));
    for worker in &observation.worker_details {
        let label = [worker.name.as_str()];
        m.worker_up
            .with_label_values(&label)
            .set(i64::from(worker.up));
        m.worker_generation
            .with_label_values(&label)
            .set(clamp(worker.generation));
        m.worker_active_shards
            .with_label_values(&label)
            .set(clamp(worker.active_shards));
        m.worker_assigned_shards
            .with_label_values(&label)
            .set(clamp(worker.assigned_shards));
        m.worker_populated_positions
            .with_label_values(&label)
            .set(clamp(worker.populated_positions));
    }
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
            allowlisted_endpoint(&Method::GET, "/memo/metadata"),
            Some("metadata")
        );
        assert_eq!(
            allowlisted_endpoint(&Method::GET, "/memo/public-params"),
            Some("public_params")
        );
        assert_eq!(
            allowlisted_endpoint(&Method::POST, "/memo/query"),
            Some("query")
        );
        assert_eq!(allowlisted_endpoint(&Method::GET, "/memo/query"), None);
        assert_eq!(allowlisted_endpoint(&Method::GET, "/metrics"), None);
        assert_eq!(allowlisted_endpoint(&Method::GET, "/ready"), None);
        assert_eq!(
            allowlisted_endpoint(&Method::GET, "/memo/metadata/../secret"),
            None
        );
    }

    #[tokio::test]
    async fn middleware_counts_allowlisted_requests_and_balances_in_flight() {
        let app = Router::new()
            .route("/memo/metadata", get(|| async { "ok" }))
            .route(
                "/memo/query",
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

        let before_metadata = counter("metadata", "GET", "200");
        let before_query = counter("query", "POST", "503");
        for (method, path) in [
            (Method::GET, "/memo/metadata"),
            (Method::POST, "/memo/query"),
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

        assert_eq!(counter("metadata", "GET", "200"), before_metadata + 1);
        assert_eq!(counter("query", "POST", "503"), before_query + 1);
        let m = metrics();
        assert_eq!(m.http_in_flight.with_label_values(&["metadata"]).get(), 0);
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
        assert!(body.contains("memo_http_requests_total{endpoint=\"metadata\""));
        assert!(!body.contains("/other"));
    }

    #[test]
    fn observation_populates_snapshot_gauges() {
        record_observation(&Observation {
            phase: Some(CoordinatorPhase::Syncing {
                current_height: 10,
                target_height: 20,
            }),
            workers: 2,
            ..Default::default()
        });
        let m = metrics();
        assert_eq!(m.snapshot_phase_code.get(), 0);
        assert_eq!(m.snapshot_sync_current_height.get(), 10);
        assert_eq!(m.snapshot_sync_target_height.get(), 20);

        record_observation(&Observation {
            phase: Some(CoordinatorPhase::Serving),
            anchor_height: 3_000_000,
            generation: 3_000_000,
            ironwood_tree_size: u64::MAX,
            used_rows: 7,
            shards: 3,
            workers: 2,
            query_slots_available: 2,
            worker_details: vec![
                WorkerObservation {
                    name: "worker-1".into(),
                    up: true,
                    generation: 3_000_000,
                    active_shards: 2,
                    assigned_shards: 2,
                    populated_positions: 100,
                },
                WorkerObservation {
                    name: "worker-2".into(),
                    up: false,
                    ..Default::default()
                },
            ],
        });
        assert_eq!(m.snapshot_phase_code.get(), 2);
        assert_eq!(m.snapshot_sync_current_height.get(), 0);
        assert_eq!(m.snapshot_anchor_height.get(), 3_000_000);
        assert_eq!(m.snapshot_ironwood_tree_size.get(), i64::MAX);
        assert_eq!(m.snapshot_query_slots_available.get(), 2);
        let (_, _, body) = encode();
        assert!(body.contains("memo_snapshot_phase_code 2"));
        assert!(body.contains("memo_snapshot_shards 3"));
        assert!(body.contains("memo_worker_up{worker=\"worker-1\"} 1"));
        assert!(body.contains("memo_worker_up{worker=\"worker-2\"} 0"));
        assert!(body.contains("memo_worker_assigned_shards{worker=\"worker-1\"} 2"));
    }
}
