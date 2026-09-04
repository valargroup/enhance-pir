# PIR APM

`pir-apm` is the operational sidecar for the active Enhance PIR coordinator.
It scrapes the coordinator's loopback-only Prometheus endpoint, renders a
dashboard, evaluates availability and latency thresholds, and can emit Slack
alerts.

Defaults match the Enhance API and `enhance_*` metric families. The sidecar
only consumes fixed endpoint labels and aggregate fleet gauges; it never reads
query bodies or client identifiers.

The dashboard also renders one query-path section per configured worker. These
five-minute windows are measured by the coordinator around each private worker
evaluation RPC and response decode. Request rate, inflight attempts, terminal
failures, and successful-attempt latency are therefore comparable across the
replicas without running another sidecar or exposing a worker metrics port.

Query latency is rendered as three nested scopes. **Observed total** runs from
request headers reaching the coordinator until the response is ready and
includes body receive time. **Post-body server** starts as soon as the complete
body is available and includes admission queueing, coordinator work, the worker
RPC, and response packing. Each worker card is the RPC subset of post-body
server time. Percentiles are calculated independently and cannot be subtracted
to derive the time spent between scopes.

Important environment variables include `PIR_APM_SCRAPE_URL`,
`PIR_APM_LISTEN`, `PIR_APM_ENVIRONMENT`, and optional
`PIR_APM_SLACK_WEBHOOK_URL`. The production unit and generated environment
live under `ops/infra/digitalocean/production/deploy` and
`ops/scripts/deploy-enhance-pir.sh`.
