//! Which Prometheus families the sidecar reads and how each endpoint is judged.
//!
//! The scraped server decides the metric prefix and the endpoint allowlist;
//! this crate only needs to know the prefix, the endpoint names, which
//! endpoints report a separate post-upload "processing" distribution, and the
//! p99 budget each endpoint is paged on.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub prefix: String,
    pub endpoints: Vec<String>,
    pub processing_endpoints: BTreeSet<String>,
    pub default_latency_p99: f64,
    pub latency_overrides: BTreeMap<String, f64>,
    pub requests_total: String,
    pub duration_bucket: String,
    pub duration_sum: String,
    pub duration_count: String,
    pub processing_bucket: String,
    pub processing_sum: String,
    pub processing_count: String,
    pub in_flight: String,
    pub processing_in_flight: String,
    pub gauge_prefix: String,
}

impl Schema {
    pub fn new(
        prefix: &str,
        endpoints: Vec<String>,
        processing_endpoints: BTreeSet<String>,
        default_latency_p99: f64,
        latency_overrides: BTreeMap<String, f64>,
    ) -> Result<Self, String> {
        if !is_identifier(prefix) {
            return Err(format!("metric prefix {prefix:?} must match [a-z0-9_]+"));
        }
        if endpoints.is_empty() {
            return Err("at least one endpoint is required".to_string());
        }
        let mut seen = BTreeSet::new();
        for endpoint in &endpoints {
            if !is_identifier(endpoint) {
                return Err(format!("endpoint {endpoint:?} must match [a-z0-9_]+"));
            }
            if !seen.insert(endpoint.as_str()) {
                return Err(format!("duplicate endpoint {endpoint:?}"));
            }
        }
        for endpoint in &processing_endpoints {
            if !seen.contains(endpoint.as_str()) {
                return Err(format!(
                    "processing endpoint {endpoint:?} is not in the endpoint list"
                ));
            }
        }
        for (endpoint, budget) in &latency_overrides {
            if !seen.contains(endpoint.as_str()) {
                return Err(format!(
                    "latency override for {endpoint:?} names an unknown endpoint"
                ));
            }
            if !(budget.is_finite() && *budget > 0.0) {
                return Err(format!("latency budget for {endpoint:?} must be positive"));
            }
        }
        if !(default_latency_p99.is_finite() && default_latency_p99 > 0.0) {
            return Err("default latency budget must be positive".to_string());
        }
        Ok(Self {
            requests_total: format!("{prefix}_http_requests_total"),
            duration_bucket: format!("{prefix}_http_request_duration_seconds_bucket"),
            duration_sum: format!("{prefix}_http_request_duration_seconds_sum"),
            duration_count: format!("{prefix}_http_request_duration_seconds_count"),
            processing_bucket: format!("{prefix}_http_request_processing_duration_seconds_bucket"),
            processing_sum: format!("{prefix}_http_request_processing_duration_seconds_sum"),
            processing_count: format!("{prefix}_http_request_processing_duration_seconds_count"),
            in_flight: format!("{prefix}_http_in_flight"),
            processing_in_flight: format!("{prefix}_http_processing_in_flight"),
            gauge_prefix: format!("{prefix}_snapshot_"),
            prefix: prefix.to_string(),
            endpoints,
            processing_endpoints,
            default_latency_p99,
            latency_overrides,
        })
    }

    /// The memo PIR coordinator layout; also the built-in default.
    pub fn memo_default() -> Self {
        Self::new(
            "memo",
            ["metadata", "params", "public_params", "query"]
                .into_iter()
                .map(String::from)
                .collect(),
            BTreeSet::from(["query".to_string()]),
            crate::thresholds::DEFAULT_LATENCY_P99_SECONDS,
            BTreeMap::from([
                ("query".to_string(), 5.0),
                ("public_params".to_string(), 2.0),
            ]),
        )
        .expect("built-in schema is valid")
    }

    pub fn knows(&self, endpoint: &str) -> bool {
        self.endpoints.iter().any(|known| known == endpoint)
    }

    /// Whether this endpoint is paged on its post-upload processing latency
    /// rather than the observed end-to-end latency.
    pub fn uses_processing(&self, endpoint: &str) -> bool {
        self.processing_endpoints.contains(endpoint)
    }

    /// The p99 budget the alert engine applies to this endpoint.
    pub fn latency_budget(&self, endpoint: &str) -> Option<f64> {
        if !self.knows(endpoint) {
            return None;
        }
        Some(
            self.latency_overrides
                .get(endpoint)
                .copied()
                .unwrap_or(self.default_latency_p99),
        )
    }

    /// Budget for the observed-latency table. Processing endpoints get none
    /// there because their observed latency is informational only.
    pub fn observed_budget(&self, endpoint: &str) -> Option<f64> {
        if self.uses_processing(endpoint) {
            None
        } else {
            self.latency_budget(endpoint)
        }
    }

    pub fn latency_label(&self, endpoint: &str) -> &'static str {
        if self.uses_processing(endpoint) {
            "processing p99"
        } else {
            "p99"
        }
    }
}

fn is_identifier(value: &str) -> bool {
    !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builds_family_names_from_the_prefix() {
        let schema = Schema::memo_default();
        assert_eq!(schema.requests_total, "memo_http_requests_total");
        assert_eq!(
            schema.processing_bucket,
            "memo_http_request_processing_duration_seconds_bucket"
        );
        assert_eq!(schema.gauge_prefix, "memo_snapshot_");
        assert_eq!(schema.latency_budget("query"), Some(5.0));
        assert_eq!(schema.latency_budget("metadata"), Some(1.0));
        assert_eq!(schema.latency_budget("nope"), None);
        assert_eq!(schema.observed_budget("query"), None);
        assert_eq!(schema.latency_label("query"), "processing p99");
        assert_eq!(schema.latency_label("params"), "p99");
    }

    #[test]
    fn rejects_inconsistent_definitions() {
        let endpoints = vec!["a".to_string()];
        assert!(Schema::new(
            "Bad-Prefix",
            endpoints.clone(),
            BTreeSet::new(),
            1.0,
            BTreeMap::new()
        )
        .is_err());
        assert!(Schema::new("ok", vec![], BTreeSet::new(), 1.0, BTreeMap::new()).is_err());
        assert!(Schema::new(
            "ok",
            endpoints.clone(),
            BTreeSet::from(["b".to_string()]),
            1.0,
            BTreeMap::new()
        )
        .is_err());
        assert!(Schema::new(
            "ok",
            endpoints.clone(),
            BTreeSet::new(),
            1.0,
            BTreeMap::from([("b".to_string(), 1.0)])
        )
        .is_err());
        assert!(Schema::new(
            "ok",
            endpoints.clone(),
            BTreeSet::new(),
            1.0,
            BTreeMap::from([("a".to_string(), 0.0)])
        )
        .is_err());
        assert!(Schema::new(
            "ok",
            vec!["a".into(), "a".into()],
            BTreeSet::new(),
            1.0,
            BTreeMap::new()
        )
        .is_err());
        assert!(Schema::new("ok", endpoints, BTreeSet::new(), 1.0, BTreeMap::new()).is_ok());
    }
}
