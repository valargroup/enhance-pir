//! Which Prometheus families the sidecar reads and how each endpoint is judged.
//!
//! The scraped server decides the metric prefix and the endpoint allowlist;
//! this crate only needs to know the prefix, the endpoint names, which
//! endpoints report a separate post-body "processing" distribution, and the
//! p99 budget each endpoint is paged on.

use std::collections::{BTreeMap, BTreeSet};

#[derive(Clone, Debug, PartialEq)]
pub struct Schema {
    pub prefix: String,
    pub endpoints: Vec<String>,
    pub processing_endpoints: BTreeSet<String>,
    /// Shown in the endpoint table but never paged (e.g. a health probe that
    /// returns 503 by design while syncing).
    pub informational_endpoints: BTreeSet<String>,
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
    /// Families like `<prefix>_worker_up{worker="..."}` describe the fleet.
    pub worker_prefix: String,
    /// Unlabelled `<prefix>_layout_*` chain-level constants.
    pub layout_prefix: String,
    /// `<prefix>_table_*{table="..."}` families describing each PIR table.
    pub table_prefix: String,
    /// `<prefix>_worker_table_*{worker="...",table="..."}` ownership families.
    pub worker_table_prefix: String,
    /// `<prefix>_worker_group_*{table="...",group="..."}` redundancy families.
    pub worker_group_prefix: String,
    pub worker_replica_requests_total: String,
    pub worker_replica_duration_bucket: String,
    pub worker_replica_duration_sum: String,
    pub worker_replica_duration_count: String,
    pub worker_replica_in_flight: String,
}

impl Schema {
    pub fn new(
        prefix: &str,
        endpoints: Vec<String>,
        processing_endpoints: BTreeSet<String>,
        informational_endpoints: BTreeSet<String>,
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
        for endpoint in processing_endpoints
            .iter()
            .chain(informational_endpoints.iter())
        {
            if !is_identifier(endpoint) {
                return Err(format!("endpoint {endpoint:?} must match [a-z0-9_]+"));
            }
        }
        // Overrides may name endpoints that only appear once a table ships,
        // since endpoint labels are discovered from the exposition; only the
        // name shape and the budget value are checked.
        for (endpoint, budget) in &latency_overrides {
            if !is_identifier(endpoint) {
                return Err(format!(
                    "latency override for {endpoint:?} must match [a-z0-9_]+"
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
            worker_prefix: format!("{prefix}_worker_"),
            layout_prefix: format!("{prefix}_layout_"),
            table_prefix: format!("{prefix}_table_"),
            worker_table_prefix: format!("{prefix}_worker_table_"),
            worker_group_prefix: format!("{prefix}_worker_group_"),
            worker_replica_requests_total: format!("{prefix}_worker_replica_requests_total"),
            worker_replica_duration_bucket: format!(
                "{prefix}_worker_replica_request_duration_seconds_bucket"
            ),
            worker_replica_duration_sum: format!(
                "{prefix}_worker_replica_request_duration_seconds_sum"
            ),
            worker_replica_duration_count: format!(
                "{prefix}_worker_replica_request_duration_seconds_count"
            ),
            worker_replica_in_flight: format!("{prefix}_worker_replica_in_flight"),
            prefix: prefix.to_string(),
            endpoints,
            processing_endpoints,
            informational_endpoints,
            default_latency_p99,
            latency_overrides,
        })
    }

    /// The enhance PIR coordinator layout; also the built-in default.
    pub fn enhance_default() -> Self {
        Self::new(
            "enhance",
            ["health", "init", "query"]
                .into_iter()
                .map(String::from)
                .collect(),
            BTreeSet::from(["query".to_string()]),
            BTreeSet::from(["health".to_string()]),
            crate::thresholds::DEFAULT_LATENCY_P99_SECONDS,
            BTreeMap::from([("query".to_string(), 5.0), ("init".to_string(), 2.0)]),
        )
        .expect("built-in schema is valid")
    }

    pub fn knows(&self, endpoint: &str) -> bool {
        self.endpoints.iter().any(|known| known == endpoint)
    }

    /// Whether this endpoint is paged on its post-body server latency
    /// rather than the observed end-to-end latency.
    pub fn uses_processing(&self, endpoint: &str) -> bool {
        self.processing_endpoints.contains(endpoint)
    }

    /// Whether the endpoint is displayed but never paged.
    pub fn is_informational(&self, endpoint: &str) -> bool {
        self.informational_endpoints.contains(endpoint)
    }

    /// The p99 budget the alert engine applies to this endpoint. Endpoints
    /// discovered from the exposition rather than configured get the default.
    pub fn latency_budget(&self, endpoint: &str) -> Option<f64> {
        if self.is_informational(endpoint) {
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
        let schema = Schema::enhance_default();
        assert_eq!(schema.requests_total, "enhance_http_requests_total");
        assert_eq!(
            schema.processing_bucket,
            "enhance_http_request_processing_duration_seconds_bucket"
        );
        assert_eq!(schema.gauge_prefix, "enhance_snapshot_");
        assert_eq!(schema.worker_prefix, "enhance_worker_");
        assert_eq!(schema.layout_prefix, "enhance_layout_");
        assert_eq!(schema.latency_budget("query"), Some(5.0));
        assert_eq!(schema.latency_budget("metadata"), Some(1.0));
        assert_eq!(schema.latency_budget("witness_query"), Some(1.0));
        assert_eq!(schema.latency_budget("health"), None);
        assert!(schema.is_informational("health"));
        assert_eq!(schema.table_prefix, "enhance_table_");
        assert_eq!(schema.worker_table_prefix, "enhance_worker_table_");
        assert_eq!(schema.worker_group_prefix, "enhance_worker_group_");
        assert_eq!(
            schema.worker_replica_requests_total,
            "enhance_worker_replica_requests_total"
        );
        assert_eq!(
            schema.worker_replica_duration_bucket,
            "enhance_worker_replica_request_duration_seconds_bucket"
        );
        assert_eq!(
            schema.worker_replica_in_flight,
            "enhance_worker_replica_in_flight"
        );
        assert_eq!(schema.observed_budget("query"), None);
    }

    #[test]
    fn rejects_inconsistent_definitions() {
        let endpoints = vec!["a".to_string()];
        assert!(Schema::new(
            "Bad-Prefix",
            endpoints.clone(),
            BTreeSet::new(),
            BTreeSet::new(),
            1.0,
            BTreeMap::new()
        )
        .is_err());
        assert!(Schema::new(
            "ok",
            vec![],
            BTreeSet::new(),
            BTreeSet::new(),
            1.0,
            BTreeMap::new()
        )
        .is_err());
        assert!(Schema::new(
            "ok",
            endpoints.clone(),
            BTreeSet::from(["Bad Name".to_string()]),
            BTreeSet::new(),
            1.0,
            BTreeMap::new()
        )
        .is_err());
        // An override for an endpoint that is not configured is fine (it may
        // be discovered later); a malformed name is not.
        assert!(Schema::new(
            "ok",
            endpoints.clone(),
            BTreeSet::new(),
            BTreeSet::new(),
            1.0,
            BTreeMap::from([("b".to_string(), 1.0)])
        )
        .is_ok());
        assert!(Schema::new(
            "ok",
            endpoints.clone(),
            BTreeSet::new(),
            BTreeSet::new(),
            1.0,
            BTreeMap::from([("Bad Name".to_string(), 1.0)])
        )
        .is_err());
        assert!(Schema::new(
            "ok",
            endpoints.clone(),
            BTreeSet::new(),
            BTreeSet::new(),
            1.0,
            BTreeMap::from([("a".to_string(), 0.0)])
        )
        .is_err());
        assert!(Schema::new(
            "ok",
            vec!["a".into(), "a".into()],
            BTreeSet::new(),
            BTreeSet::new(),
            1.0,
            BTreeMap::new()
        )
        .is_err());
        assert!(Schema::new(
            "ok",
            endpoints,
            BTreeSet::new(),
            BTreeSet::new(),
            1.0,
            BTreeMap::new()
        )
        .is_ok());
    }
}
