# PIR APM

`pir-apm` is the operational sidecar for the active Enhance PIR coordinator.
It scrapes the coordinator's loopback-only Prometheus endpoint, renders a
dashboard, evaluates availability and latency thresholds, and can emit Slack
alerts.

Defaults match the Enhance API and `enhance_*` metric families. The sidecar
only consumes fixed endpoint labels and aggregate fleet gauges; it never reads
query bodies or client identifiers.

Important environment variables include `PIR_APM_SCRAPE_URL`,
`PIR_APM_LISTEN`, `PIR_APM_ENVIRONMENT`, and optional
`PIR_APM_SLACK_WEBHOOK_URL`. The production unit and generated environment
live under `ops/infra/digitalocean/production/deploy` and
`ops/scripts/deploy-enhance-pir.sh`.
