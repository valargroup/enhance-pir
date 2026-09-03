use std::{
    collections::{BTreeMap, HashMap, VecDeque},
    time::Instant,
};

use crate::schema::Schema;

const MAX_SNAPSHOTS: usize = 21;

#[derive(Clone, Debug, Default)]
pub struct HistogramCumulative {
    pub buckets: Vec<(f64, f64)>,
    pub sum: f64,
    pub count: f64,
}

#[derive(Clone, Debug, Default)]
pub struct EndpointCumulative {
    pub requests: f64,
    pub errors_5xx: f64,
    pub observed: HistogramCumulative,
    pub processing: HistogramCumulative,
    pub in_flight: f64,
    pub processing_in_flight: f64,
}

#[derive(Clone, Debug)]
pub struct MetricsSnapshot {
    pub at: Instant,
    pub endpoints: BTreeMap<String, EndpointCumulative>,
    pub snapshot_gauges: BTreeMap<String, f64>,
    /// worker name -> (gauge stem such as `up`, value)
    pub workers: BTreeMap<String, BTreeMap<String, f64>>,
    /// gauge stem such as `confirmations` -> value
    pub layout: BTreeMap<String, f64>,
    /// table name -> (gauge stem such as `shards`, value)
    pub tables: BTreeMap<String, BTreeMap<String, f64>>,
    /// worker name -> table name -> (gauge stem, value)
    pub worker_tables: BTreeMap<String, BTreeMap<String, BTreeMap<String, f64>>>,
    /// table name -> group name -> (redundancy gauge stem, value)
    pub worker_groups: BTreeMap<String, BTreeMap<String, BTreeMap<String, f64>>>,
    pub resident_memory_bytes: Option<f64>,
    pub process_start_time_seconds: Option<f64>,
}

#[derive(Clone, Debug, Default)]
pub struct LatencyWindow {
    pub samples: f64,
    pub p50: Option<f64>,
    pub p95: Option<f64>,
    pub p99: Option<f64>,
}

#[derive(Clone, Debug, Default)]
pub struct EndpointWindow {
    pub qps: f64,
    pub requests: f64,
    pub errors_5xx: f64,
    pub error_ratio: f64,
    pub observed: LatencyWindow,
    pub processing: LatencyWindow,
    pub in_flight: f64,
    pub processing_in_flight: f64,
    /// The server reported a processing histogram for this endpoint, so its
    /// alert can use processing latency even without configuration.
    pub processing_available: bool,
}

impl EndpointWindow {
    /// Latency distribution used for paging this endpoint.
    pub fn alert_latency(&self, uses_processing: bool) -> &LatencyWindow {
        if uses_processing {
            &self.processing
        } else {
            &self.observed
        }
    }
}

pub struct RollingMetrics {
    schema: Schema,
    snapshots: VecDeque<MetricsSnapshot>,
}

impl RollingMetrics {
    pub fn new(schema: Schema) -> Self {
        Self {
            schema,
            snapshots: VecDeque::new(),
        }
    }

    pub fn push(&mut self, snapshot: MetricsSnapshot) {
        self.snapshots.push_back(snapshot);
        while self.snapshots.len() > MAX_SNAPSHOTS {
            self.snapshots.pop_front();
        }
    }

    pub fn latest(&self) -> Option<&MetricsSnapshot> {
        self.snapshots.back()
    }

    pub fn windows(&self) -> BTreeMap<String, EndpointWindow> {
        let Some(newest) = self.snapshots.back() else {
            return BTreeMap::new();
        };
        let oldest_index = self
            .snapshots
            .iter()
            .position(|snapshot| newest.at.duration_since(snapshot.at).as_secs() <= 300)
            .unwrap_or(self.snapshots.len() - 1);
        let oldest = &self.snapshots[oldest_index];
        let elapsed = newest.at.duration_since(oldest.at).as_secs_f64().max(1.0);

        // Configured endpoints always get a window; anything else the server
        // labelled (its label set is closed) is appended so new tables show
        // up without a config change.
        let mut names: Vec<&str> = self.schema.endpoints.iter().map(String::as_str).collect();
        names.extend(
            newest
                .endpoints
                .keys()
                .map(String::as_str)
                .filter(|name| !self.schema.knows(name)),
        );
        names
            .into_iter()
            .map(|endpoint| {
                let current = newest.endpoints.get(endpoint).cloned().unwrap_or_default();
                let mut requests = 0.0;
                let mut errors = 0.0;
                for index in (oldest_index + 1)..self.snapshots.len() {
                    let previous_snapshot = &self.snapshots[index - 1];
                    let next_snapshot = &self.snapshots[index];
                    let reset = process_generation_changed(previous_snapshot, next_snapshot);
                    let previous = previous_snapshot
                        .endpoints
                        .get(endpoint)
                        .cloned()
                        .unwrap_or_default();
                    let next = next_snapshot
                        .endpoints
                        .get(endpoint)
                        .cloned()
                        .unwrap_or_default();
                    requests += counter_delta(next.requests, previous.requests, reset);
                    errors += counter_delta(next.errors_5xx, previous.errors_5xx, reset);
                }
                (
                    endpoint.to_string(),
                    EndpointWindow {
                        qps: requests / elapsed,
                        requests,
                        errors_5xx: errors,
                        error_ratio: if requests > 0.0 {
                            errors / requests
                        } else {
                            0.0
                        },
                        observed: latency_window(
                            &self.snapshots,
                            oldest_index,
                            endpoint,
                            observed_histogram,
                        ),
                        processing: latency_window(
                            &self.snapshots,
                            oldest_index,
                            endpoint,
                            processing_histogram,
                        ),
                        in_flight: current.in_flight,
                        processing_in_flight: current.processing_in_flight,
                        processing_available: current.processing.count > 0.0,
                    },
                )
            })
            .collect()
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.snapshots.len()
    }
}

fn observed_histogram(values: &EndpointCumulative) -> &HistogramCumulative {
    &values.observed
}

fn processing_histogram(values: &EndpointCumulative) -> &HistogramCumulative {
    &values.processing
}

fn latency_window(
    snapshots: &VecDeque<MetricsSnapshot>,
    oldest_index: usize,
    endpoint: &str,
    histogram: fn(&EndpointCumulative) -> &HistogramCumulative,
) -> LatencyWindow {
    let current_endpoint = snapshots
        .back()
        .and_then(|snapshot| snapshot.endpoints.get(endpoint))
        .cloned()
        .unwrap_or_default();
    let mut buckets: Vec<(f64, f64)> = histogram(&current_endpoint)
        .buckets
        .iter()
        .map(|(upper, _)| (*upper, 0.0))
        .collect();
    let mut samples = 0.0;
    for index in (oldest_index + 1)..snapshots.len() {
        let previous_snapshot = &snapshots[index - 1];
        let next_snapshot = &snapshots[index];
        let generation_changed = process_generation_changed(previous_snapshot, next_snapshot);
        let previous = previous_snapshot
            .endpoints
            .get(endpoint)
            .cloned()
            .unwrap_or_default();
        let next = next_snapshot
            .endpoints
            .get(endpoint)
            .cloned()
            .unwrap_or_default();
        let previous = histogram(&previous);
        let next = histogram(&next);
        let reset = generation_changed || next.count < previous.count;
        samples += if reset {
            next.count
        } else {
            next.count - previous.count
        };
        // A histogram resets as one metric family. Differencing its buckets
        // independently across a process restart can make them non-monotonic.
        let delta = if reset {
            next.buckets.clone()
        } else {
            histogram_delta(&next.buckets, &previous.buckets)
        };
        for (upper, total) in &mut buckets {
            *total += delta
                .iter()
                .find(|(delta_upper, _)| delta_upper == upper)
                .map(|(_, value)| *value)
                .unwrap_or(0.0);
        }
    }
    LatencyWindow {
        samples,
        p50: histogram_quantile(0.50, &buckets, samples),
        p95: histogram_quantile(0.95, &buckets, samples),
        p99: histogram_quantile(0.99, &buckets, samples),
    }
}

fn process_generation_changed(previous: &MetricsSnapshot, next: &MetricsSnapshot) -> bool {
    matches!(
        (
            previous.process_start_time_seconds,
            next.process_start_time_seconds,
        ),
        (Some(previous), Some(next)) if previous != next
    )
}

fn counter_delta(current: f64, previous: f64, reset: bool) -> f64 {
    if reset || current < previous {
        current
    } else {
        current - previous
    }
}

pub fn histogram_delta(current: &[(f64, f64)], previous: &[(f64, f64)]) -> Vec<(f64, f64)> {
    current
        .iter()
        .map(|(upper, value)| {
            let old = previous
                .iter()
                .find(|(old_upper, _)| old_upper == upper)
                .map(|(_, value)| *value)
                .unwrap_or(0.0);
            (*upper, counter_delta(*value, old, false))
        })
        .collect()
}

pub fn histogram_quantile(q: f64, cumulative_buckets: &[(f64, f64)], count: f64) -> Option<f64> {
    if !(0.0..=1.0).contains(&q) || count <= 0.0 || cumulative_buckets.is_empty() {
        return None;
    }
    let rank = q * count;
    let mut previous_count = 0.0;
    let mut previous_upper = 0.0;
    for (upper, bucket_count) in cumulative_buckets {
        if *bucket_count >= rank {
            if upper.is_infinite() {
                return Some(previous_upper);
            }
            let observations = (*bucket_count - previous_count).max(0.0);
            if observations == 0.0 {
                return Some(*upper);
            }
            return Some(
                previous_upper + (*upper - previous_upper) * (rank - previous_count) / observations,
            );
        }
        previous_count = *bucket_count;
        if upper.is_finite() {
            previous_upper = *upper;
        }
    }
    Some(previous_upper)
}

#[derive(Debug)]
struct ParsedSample {
    name: String,
    labels: HashMap<String, String>,
    value: f64,
}

pub fn parse_prometheus(
    schema: &Schema,
    text: &str,
    at: Instant,
) -> Result<MetricsSnapshot, String> {
    let mut endpoints: BTreeMap<String, EndpointCumulative> = schema
        .endpoints
        .iter()
        .map(|name| (name.clone(), EndpointCumulative::default()))
        .collect();
    let mut snapshot_gauges = BTreeMap::new();
    let mut workers: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    let mut layout: BTreeMap<String, f64> = BTreeMap::new();
    let mut tables: BTreeMap<String, BTreeMap<String, f64>> = BTreeMap::new();
    let mut worker_tables: BTreeMap<String, BTreeMap<String, BTreeMap<String, f64>>> =
        BTreeMap::new();
    let mut worker_groups: BTreeMap<String, BTreeMap<String, BTreeMap<String, f64>>> =
        BTreeMap::new();
    let mut resident_memory_bytes = None;
    let mut process_start_time_seconds = None;

    for (line_number, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let sample = parse_line(line)
            .map_err(|error| format!("line {}: {error}", line_number.saturating_add(1)))?;
        let name = sample.name.as_str();
        if name == schema.requests_total {
            if let Some(values) = endpoint_entry(&mut endpoints, &sample) {
                values.requests += sample.value;
                if sample
                    .labels
                    .get("status")
                    .is_some_and(|status| status.starts_with('5'))
                {
                    values.errors_5xx += sample.value;
                }
            }
        } else if name == schema.duration_bucket {
            set_histogram_bucket(&mut endpoints, &sample, |values| &mut values.observed)?;
        } else if name == schema.duration_sum {
            set_histogram_value(
                &mut endpoints,
                &sample,
                |values| &mut values.observed,
                |h| h.sum = sample.value,
            );
        } else if name == schema.duration_count {
            set_histogram_value(
                &mut endpoints,
                &sample,
                |values| &mut values.observed,
                |h| h.count = sample.value,
            );
        } else if name == schema.processing_bucket {
            set_histogram_bucket(&mut endpoints, &sample, |values| &mut values.processing)?;
        } else if name == schema.processing_sum {
            set_histogram_value(
                &mut endpoints,
                &sample,
                |values| &mut values.processing,
                |histogram| histogram.sum = sample.value,
            );
        } else if name == schema.processing_count {
            set_histogram_value(
                &mut endpoints,
                &sample,
                |values| &mut values.processing,
                |histogram| histogram.count = sample.value,
            );
        } else if name == schema.in_flight {
            set_endpoint_value(&mut endpoints, &sample, |values| {
                values.in_flight = sample.value
            });
        } else if name == schema.processing_in_flight {
            set_endpoint_value(&mut endpoints, &sample, |values| {
                values.processing_in_flight = sample.value
            });
        } else if name == "process_resident_memory_bytes" {
            resident_memory_bytes = Some(sample.value);
        } else if name == "process_start_time_seconds" {
            process_start_time_seconds = Some(sample.value);
        } else if name.starts_with(&schema.gauge_prefix) {
            snapshot_gauges.insert(name.to_string(), sample.value);
        } else if let Some(stem) = name.strip_prefix(&schema.worker_group_prefix) {
            if let (Some(table), Some(group)) =
                (sample.labels.get("table"), sample.labels.get("group"))
            {
                worker_groups
                    .entry(table.clone())
                    .or_default()
                    .entry(group.clone())
                    .or_default()
                    .insert(stem.to_string(), sample.value);
            }
        } else if let Some(stem) = name.strip_prefix(&schema.worker_table_prefix) {
            if let (Some(worker), Some(table)) =
                (sample.labels.get("worker"), sample.labels.get("table"))
            {
                worker_tables
                    .entry(worker.clone())
                    .or_default()
                    .entry(table.clone())
                    .or_default()
                    .insert(stem.to_string(), sample.value);
            }
        } else if let Some(stem) = name.strip_prefix(&schema.table_prefix) {
            if let Some(table) = sample.labels.get("table") {
                tables
                    .entry(table.clone())
                    .or_default()
                    .insert(stem.to_string(), sample.value);
            }
        } else if let Some(stem) = name.strip_prefix(&schema.worker_prefix) {
            if let Some(worker) = sample.labels.get("worker") {
                workers
                    .entry(worker.clone())
                    .or_default()
                    .insert(stem.to_string(), sample.value);
            }
        } else if let Some(stem) = name.strip_prefix(&schema.layout_prefix) {
            layout.insert(stem.to_string(), sample.value);
        }
    }
    for endpoint in endpoints.values_mut() {
        endpoint
            .observed
            .buckets
            .sort_by(|(left, _), (right, _)| left.total_cmp(right));
        endpoint
            .processing
            .buckets
            .sort_by(|(left, _), (right, _)| left.total_cmp(right));
    }
    Ok(MetricsSnapshot {
        at,
        endpoints,
        snapshot_gauges,
        workers,
        layout,
        tables,
        worker_tables,
        worker_groups,
        resident_memory_bytes,
        process_start_time_seconds,
    })
}

/// Most distinct endpoint labels accepted from one exposition. The server's
/// label set is closed, so this only bounds a misbehaving scrape target.
const MAX_ENDPOINTS: usize = 32;

fn is_endpoint_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 40
        && label
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

/// The cumulative slot for a sample's `endpoint` label, creating one for a
/// label the config did not list as long as it is well formed and the cap
/// has not been reached.
fn endpoint_entry<'a>(
    endpoints: &'a mut BTreeMap<String, EndpointCumulative>,
    sample: &ParsedSample,
) -> Option<&'a mut EndpointCumulative> {
    let endpoint = sample.labels.get("endpoint")?;
    if !endpoints.contains_key(endpoint) {
        if !is_endpoint_label(endpoint) || endpoints.len() >= MAX_ENDPOINTS {
            return None;
        }
        endpoints.insert(endpoint.clone(), EndpointCumulative::default());
    }
    endpoints.get_mut(endpoint)
}

fn set_histogram_bucket(
    endpoints: &mut BTreeMap<String, EndpointCumulative>,
    sample: &ParsedSample,
    histogram: fn(&mut EndpointCumulative) -> &mut HistogramCumulative,
) -> Result<(), String> {
    let Some(le) = sample.labels.get("le").cloned() else {
        return Ok(());
    };
    let Some(values) = endpoint_entry(endpoints, sample) else {
        return Ok(());
    };
    let upper = if le == "+Inf" {
        f64::INFINITY
    } else {
        le.parse::<f64>()
            .map_err(|_| format!("invalid histogram bound {le:?}"))?
    };
    histogram(values).buckets.push((upper, sample.value));
    Ok(())
}

fn set_histogram_value(
    endpoints: &mut BTreeMap<String, EndpointCumulative>,
    sample: &ParsedSample,
    histogram: fn(&mut EndpointCumulative) -> &mut HistogramCumulative,
    set: impl FnOnce(&mut HistogramCumulative),
) {
    if let Some(values) = endpoint_entry(endpoints, sample) {
        set(histogram(values));
    }
}

fn set_endpoint_value(
    endpoints: &mut BTreeMap<String, EndpointCumulative>,
    sample: &ParsedSample,
    set: impl FnOnce(&mut EndpointCumulative),
) {
    if let Some(values) = endpoint_entry(endpoints, sample) {
        set(values);
    }
}

fn parse_line(line: &str) -> Result<ParsedSample, String> {
    let split = line
        .rfind(char::is_whitespace)
        .ok_or_else(|| "missing metric value".to_string())?;
    let descriptor = line[..split].trim_end();
    let value_text = line[split..].trim();
    let value = value_text
        .parse::<f64>()
        .map_err(|_| format!("invalid metric value {value_text:?}"))?;

    let (name, labels) = if let Some(open) = descriptor.find('{') {
        if !descriptor.ends_with('}') {
            return Err("unterminated label set".to_string());
        }
        (
            descriptor[..open].to_string(),
            parse_labels(&descriptor[open + 1..descriptor.len() - 1])?,
        )
    } else {
        (descriptor.to_string(), HashMap::new())
    };
    if name.is_empty() {
        return Err("empty metric name".to_string());
    }
    Ok(ParsedSample {
        name,
        labels,
        value,
    })
}

fn parse_labels(input: &str) -> Result<HashMap<String, String>, String> {
    let mut labels = HashMap::new();
    let bytes = input.as_bytes();
    let mut cursor = 0;
    while cursor < bytes.len() {
        while cursor < bytes.len() && (bytes[cursor] == b',' || bytes[cursor].is_ascii_whitespace())
        {
            cursor += 1;
        }
        if cursor == bytes.len() {
            break;
        }
        let key_start = cursor;
        while cursor < bytes.len() && bytes[cursor] != b'=' {
            cursor += 1;
        }
        if cursor == bytes.len() {
            return Err("label missing '='".to_string());
        }
        let key = input[key_start..cursor].trim().to_string();
        cursor += 1;
        if cursor >= bytes.len() || bytes[cursor] != b'"' {
            return Err("label value must be quoted".to_string());
        }
        cursor += 1;
        let mut value = String::new();
        let mut closed = false;
        while cursor < bytes.len() {
            match bytes[cursor] {
                b'"' => {
                    cursor += 1;
                    closed = true;
                    break;
                }
                b'\\' => {
                    cursor += 1;
                    if cursor >= bytes.len() {
                        return Err("unterminated label escape".to_string());
                    }
                    value.push(match bytes[cursor] {
                        b'n' => '\n',
                        b'\\' => '\\',
                        b'"' => '"',
                        other => other as char,
                    });
                    cursor += 1;
                }
                byte => {
                    value.push(byte as char);
                    cursor += 1;
                }
            }
        }
        if !closed {
            return Err("unterminated quoted label value".to_string());
        }
        labels.insert(key, value);
    }
    Ok(labels)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn parses_required_prometheus_families_and_labels() {
        let text = r#"
# HELP ignored comment
enhance_http_requests_total{endpoint="metadata",method="GET",status="200"} 90
enhance_http_requests_total{endpoint="metadata",method="GET",status="503"} 10
enhance_http_request_duration_seconds_bucket{endpoint="metadata",le="0.5"} 60
enhance_http_request_duration_seconds_bucket{endpoint="metadata",le="1"} 95
enhance_http_request_duration_seconds_bucket{endpoint="metadata",le="+Inf"} 100
enhance_http_request_duration_seconds_sum{endpoint="metadata"} 40
enhance_http_request_duration_seconds_count{endpoint="metadata"} 100
enhance_http_in_flight{endpoint="metadata"} 3
enhance_http_request_processing_duration_seconds_bucket{endpoint="query",le="0.5"} 9
enhance_http_request_processing_duration_seconds_bucket{endpoint="query",le="+Inf"} 10
enhance_http_request_processing_duration_seconds_sum{endpoint="query"} 2
enhance_http_request_processing_duration_seconds_count{endpoint="query"} 10
enhance_http_processing_in_flight{endpoint="query"} 2
enhance_snapshot_anchor_height 123
enhance_snapshot_generation 124
nf_snapshot_ignored 1
enhance_worker_up{worker="worker-1"} 1
enhance_worker_up{worker="worker-2"} 0
enhance_worker_assigned_shards{worker="worker-1"} 2
enhance_layout_confirmations 10
enhance_table_registered{table="action"} 1
enhance_table_shards{table="action"} 3
enhance_worker_table_index{table="action",worker="worker-1"} 0
enhance_worker_table_assigned_shards{table="action",worker="worker-1"} 2
enhance_worker_group_configured_replicas{table="action",group="group-1"} 2
enhance_worker_group_ready_replicas{table="action",group="group-1"} 1
enhance_http_requests_total{endpoint="witness_query",method="POST",status="200"} 5
enhance_http_request_processing_duration_seconds_count{endpoint="witness_query"} 5
enhance_http_requests_total{endpoint="Bad Label",method="GET",status="200"} 1
process_resident_memory_bytes 1048576
process_start_time_seconds 1787880000
"#;
        let parsed = parse_prometheus(&Schema::enhance_default(), text, Instant::now()).unwrap();
        let metadata = &parsed.endpoints["metadata"];
        assert_eq!(metadata.requests, 100.0);
        assert_eq!(metadata.errors_5xx, 10.0);
        assert_eq!(metadata.in_flight, 3.0);
        assert_eq!(metadata.observed.sum, 40.0);
        assert_eq!(metadata.observed.count, 100.0);
        assert_eq!(metadata.observed.buckets.len(), 3);
        let query = &parsed.endpoints["query"];
        assert_eq!(query.processing.sum, 2.0);
        assert_eq!(query.processing.count, 10.0);
        assert_eq!(query.processing_in_flight, 2.0);
        assert_eq!(
            parsed.snapshot_gauges["enhance_snapshot_anchor_height"],
            123.0
        );
        assert_eq!(parsed.snapshot_gauges.len(), 2);
        assert_eq!(parsed.workers["worker-1"]["up"], 1.0);
        assert_eq!(parsed.workers["worker-1"]["assigned_shards"], 2.0);
        assert_eq!(parsed.workers["worker-2"]["up"], 0.0);
        assert!(!parsed.snapshot_gauges.contains_key("enhance_worker_up"));
        assert_eq!(parsed.layout["confirmations"], 10.0);
        assert_eq!(parsed.tables["action"]["registered"], 1.0);
        assert_eq!(parsed.tables["action"]["shards"], 3.0);
        assert_eq!(
            parsed.worker_groups["action"]["group-1"]["configured_replicas"],
            2.0
        );
        assert_eq!(
            parsed.worker_groups["action"]["group-1"]["ready_replicas"],
            1.0
        );
        assert_eq!(parsed.worker_tables["worker-1"]["action"]["index"], 0.0);
        assert_eq!(
            parsed.worker_tables["worker-1"]["action"]["assigned_shards"],
            2.0
        );
        assert!(!parsed.workers["worker-1"].contains_key("table_index"));
        // Discovered endpoint: not configured, but labelled by the server.
        assert_eq!(parsed.endpoints["witness_query"].requests, 5.0);
        assert_eq!(parsed.endpoints["witness_query"].processing.count, 5.0);
        assert!(!parsed.endpoints.contains_key("Bad Label"));
        assert_eq!(parsed.resident_memory_bytes, Some(1_048_576.0));
        assert_eq!(parsed.process_start_time_seconds, Some(1_787_880_000.0));
    }

    #[test]
    fn parses_escaped_label_values() {
        let sample = parse_line(r#"metric{a="quote\"",b="slash\\",c="line\n"} 7"#).unwrap();
        assert_eq!(sample.labels["a"], "quote\"");
        assert_eq!(sample.labels["b"], "slash\\");
        assert_eq!(sample.labels["c"], "line\n");
        assert!(parse_line(r#"metric{a="unterminated} 7"#).is_err());
    }

    #[test]
    fn computes_histogram_deltas_and_interpolated_quantiles() {
        let old = vec![(0.5, 10.0), (1.0, 20.0), (f64::INFINITY, 20.0)];
        let new = vec![(0.5, 20.0), (1.0, 40.0), (f64::INFINITY, 40.0)];
        let delta = histogram_delta(&new, &old);
        assert_eq!(delta[0].1, 10.0);
        assert!((histogram_quantile(0.5, &delta, 20.0).unwrap() - 0.5).abs() < 1e-9);
        assert!((histogram_quantile(0.75, &delta, 20.0).unwrap() - 0.75).abs() < 1e-9);
        assert_eq!(
            histogram_quantile(0.99, &[(1.0, 5.0), (f64::INFINITY, 10.0)], 10.0),
            Some(1.0)
        );
    }

    #[test]
    fn keeps_observed_and_processing_latency_separate() {
        let start = Instant::now();
        let histogram = |count: f64, slow_upper: f64| HistogramCumulative {
            buckets: vec![
                (slow_upper / 2.0, 0.0),
                (slow_upper, count),
                (f64::INFINITY, count),
            ],
            sum: count * slow_upper,
            count,
        };
        let mut rolling = RollingMetrics::new(Schema::enhance_default());
        for (index, count) in [0.0, 20.0].into_iter().enumerate() {
            let mut endpoints = BTreeMap::new();
            endpoints.insert(
                "query".to_string(),
                EndpointCumulative {
                    requests: count,
                    observed: histogram(count, 10.0),
                    processing: histogram(count, 0.5),
                    processing_in_flight: index as f64,
                    ..Default::default()
                },
            );
            rolling.push(MetricsSnapshot {
                at: start + Duration::from_secs(index as u64 * 15),
                endpoints,
                snapshot_gauges: BTreeMap::new(),
                workers: BTreeMap::new(),
                layout: BTreeMap::new(),
                tables: BTreeMap::new(),
                worker_tables: BTreeMap::new(),
                worker_groups: BTreeMap::new(),
                resident_memory_bytes: None,
                process_start_time_seconds: None,
            });
        }

        let window = &rolling.windows()["query"];
        assert_eq!(window.observed.samples, 20.0);
        assert_eq!(window.processing.samples, 20.0);
        assert!(window.observed.p99.unwrap() > 9.0);
        assert!(window.processing.p99.unwrap() < 0.5);
        assert_eq!(window.alert_latency(true).p99, window.processing.p99);
        assert_eq!(window.alert_latency(false).p99, window.observed.p99);
        assert_eq!(window.processing_in_flight, 1.0);
    }

    #[test]
    fn histogram_reset_uses_only_the_new_process_distribution() {
        fn before_restart(upper: f64) -> HistogramCumulative {
            HistogramCumulative {
                buckets: vec![
                    (upper, 15.0),
                    (upper * 2.0, 18.0),
                    (upper * 4.0, 20.0),
                    (f64::INFINITY, 100.0),
                ],
                sum: 500.0,
                count: 100.0,
            }
        }

        fn after_restart(upper: f64) -> HistogramCumulative {
            HistogramCumulative {
                buckets: vec![
                    (upper, 20.0),
                    (upper * 2.0, 20.0),
                    (upper * 4.0, 20.0),
                    (f64::INFINITY, 20.0),
                ],
                sum: upper * 10.0,
                count: 20.0,
            }
        }

        let start = Instant::now();
        let mut rolling = RollingMetrics::new(Schema::enhance_default());
        for index in 0..2 {
            let mut endpoints = BTreeMap::new();
            endpoints.insert(
                "query".to_string(),
                EndpointCumulative {
                    requests: if index == 0 { 100.0 } else { 20.0 },
                    observed: if index == 0 {
                        before_restart(10.0)
                    } else {
                        after_restart(10.0)
                    },
                    processing: if index == 0 {
                        before_restart(0.5)
                    } else {
                        after_restart(0.5)
                    },
                    ..Default::default()
                },
            );
            rolling.push(MetricsSnapshot {
                at: start + Duration::from_secs(index as u64 * 15),
                endpoints,
                snapshot_gauges: BTreeMap::new(),
                workers: BTreeMap::new(),
                layout: BTreeMap::new(),
                tables: BTreeMap::new(),
                worker_tables: BTreeMap::new(),
                worker_groups: BTreeMap::new(),
                resident_memory_bytes: None,
                process_start_time_seconds: None,
            });
        }

        let window = &rolling.windows()["query"];
        assert_eq!(window.requests, 20.0);
        assert_eq!(window.observed.samples, 20.0);
        assert_eq!(window.processing.samples, 20.0);
        assert!(window.observed.p99.unwrap() < 10.0);
        assert!(window.processing.p99.unwrap() < 0.5);
    }

    #[test]
    fn process_start_time_detects_restart_when_counts_increase() {
        fn histogram(count: f64, fast: f64) -> HistogramCumulative {
            HistogramCumulative {
                buckets: vec![
                    (0.5, fast),
                    (2.0, fast),
                    (5.0, count),
                    (f64::INFINITY, count),
                ],
                sum: count * 3.0,
                count,
            }
        }

        let start = Instant::now();
        let mut rolling = RollingMetrics::new(Schema::enhance_default());
        for (index, count) in [10.0, 25.0].into_iter().enumerate() {
            let mut endpoints = BTreeMap::new();
            endpoints.insert(
                "query".to_string(),
                EndpointCumulative {
                    requests: count,
                    errors_5xx: if index == 0 { 1.0 } else { 3.0 },
                    processing: histogram(count, if index == 0 { count } else { 0.0 }),
                    ..Default::default()
                },
            );
            rolling.push(MetricsSnapshot {
                at: start + Duration::from_secs(index as u64 * 15),
                endpoints,
                snapshot_gauges: BTreeMap::new(),
                workers: BTreeMap::new(),
                layout: BTreeMap::new(),
                tables: BTreeMap::new(),
                worker_tables: BTreeMap::new(),
                worker_groups: BTreeMap::new(),
                resident_memory_bytes: None,
                process_start_time_seconds: Some(100.0 + index as f64),
            });
        }

        let window = &rolling.windows()["query"];
        assert_eq!(window.requests, 25.0);
        assert_eq!(window.errors_5xx, 3.0);
        assert_eq!(window.processing.samples, 25.0);
        assert!(window.processing.p99.unwrap() > 2.0);
    }

    #[test]
    fn missing_processing_family_leaves_processing_unavailable() {
        let parsed = parse_prometheus(
            &Schema::enhance_default(),
            r#"
enhance_http_requests_total{endpoint="query",method="POST",status="200"} 20
enhance_http_request_duration_seconds_bucket{endpoint="query",le="10"} 20
enhance_http_request_duration_seconds_bucket{endpoint="query",le="+Inf"} 20
enhance_http_request_duration_seconds_sum{endpoint="query"} 180
enhance_http_request_duration_seconds_count{endpoint="query"} 20
"#,
            Instant::now(),
        )
        .unwrap();

        let query = &parsed.endpoints["query"];
        assert_eq!(query.observed.count, 20.0);
        assert_eq!(query.processing.count, 0.0);
    }

    #[test]
    fn a_different_prefix_and_endpoint_set_parses() {
        let schema = Schema::new(
            "nf",
            vec!["tier0".to_string()],
            Default::default(),
            Default::default(),
            1.0,
            Default::default(),
        )
        .unwrap();
        let parsed = parse_prometheus(
            &schema,
            r#"
nf_http_requests_total{endpoint="tier0",method="GET",status="200"} 4
nf_snapshot_served_height 9
enhance_http_requests_total{endpoint="metadata",method="GET",status="200"} 4
enhance_snapshot_generation 1
"#,
            Instant::now(),
        )
        .unwrap();
        assert_eq!(parsed.endpoints["tier0"].requests, 4.0);
        assert!(!parsed.endpoints.contains_key("metadata"));
        assert_eq!(parsed.snapshot_gauges["nf_snapshot_served_height"], 9.0);
        assert_eq!(parsed.snapshot_gauges.len(), 1);
    }

    #[test]
    fn discovered_endpoints_get_windows_after_configured_ones() {
        let schema = Schema::enhance_default();
        let mut rolling = RollingMetrics::new(schema.clone());
        let mut endpoints = BTreeMap::new();
        endpoints.insert(
            "witness_query".to_string(),
            EndpointCumulative {
                requests: 5.0,
                processing: HistogramCumulative {
                    buckets: vec![(1.0, 5.0), (f64::INFINITY, 5.0)],
                    sum: 2.0,
                    count: 5.0,
                },
                ..Default::default()
            },
        );
        rolling.push(MetricsSnapshot {
            at: Instant::now(),
            endpoints,
            snapshot_gauges: BTreeMap::new(),
            workers: BTreeMap::new(),
            layout: BTreeMap::new(),
            tables: BTreeMap::new(),
            worker_tables: BTreeMap::new(),
            worker_groups: BTreeMap::new(),
            resident_memory_bytes: None,
            process_start_time_seconds: None,
        });
        let windows = rolling.windows();
        assert_eq!(windows.len(), schema.endpoints.len() + 1);
        assert!(windows["witness_query"].processing_available);
        assert!(!windows["query"].processing_available);
    }

    #[test]
    fn endpoint_discovery_is_capped_and_validated() {
        let mut endpoints = BTreeMap::new();
        for index in 0..MAX_ENDPOINTS {
            let sample = ParsedSample {
                name: "x".into(),
                labels: HashMap::from([("endpoint".to_string(), format!("e{index}"))]),
                value: 1.0,
            };
            assert!(endpoint_entry(&mut endpoints, &sample).is_some());
        }
        let overflow = ParsedSample {
            name: "x".into(),
            labels: HashMap::from([("endpoint".to_string(), "one_more".to_string())]),
            value: 1.0,
        };
        assert!(endpoint_entry(&mut endpoints, &overflow).is_none());
        let known = ParsedSample {
            name: "x".into(),
            labels: HashMap::from([("endpoint".to_string(), "e0".to_string())]),
            value: 1.0,
        };
        assert!(endpoint_entry(&mut endpoints, &known).is_some());
    }

    #[test]
    fn handles_counter_reset_and_caps_history() {
        let start = Instant::now();
        let mut rolling = RollingMetrics::new(Schema::enhance_default());
        for index in 0..25 {
            let mut endpoints = BTreeMap::new();
            endpoints.insert(
                "metadata".to_string(),
                EndpointCumulative {
                    requests: if index == 24 { 5.0 } else { index as f64 },
                    ..Default::default()
                },
            );
            rolling.push(MetricsSnapshot {
                at: start + Duration::from_secs(index * 15),
                endpoints,
                snapshot_gauges: BTreeMap::new(),
                workers: BTreeMap::new(),
                layout: BTreeMap::new(),
                tables: BTreeMap::new(),
                worker_tables: BTreeMap::new(),
                worker_groups: BTreeMap::new(),
                resident_memory_bytes: None,
                process_start_time_seconds: None,
            });
        }
        assert_eq!(rolling.len(), 21);
        assert_eq!(rolling.windows()["metadata"].requests, 24.0);
    }
}
