# pir-apm — memo PIR APM sidecar

`pir-apm` is a small monitoring sidecar that runs next to the memo PIR
coordinator. Every 15 seconds it scrapes the coordinator's Prometheus
`/metrics`, its JSON `/memo/health`, and its `/ready` gate over loopback, keeps
five-minute rolling windows, renders a server-side HTML dashboard, and posts
fire/recover messages to Slack when a threshold is crossed.

It was ported from `vote-nullifier-pir`'s `deploy/pir-apm` and generalised so
the metric prefix, endpoint list, latency budgets, and probe paths come from the
environment instead of being compiled in.

## Where it runs

| Piece | Location on the coordinator |
| --- | --- |
| Binary | `/usr/local/bin/pir-apm` |
| Unit | `/etc/systemd/system/pir-apm.service` (from `infra/digitalocean/memo-poc/deploy/pir-apm.service`) |
| Environment | `/etc/default/pir-apm`, root-only, written by `scripts/deploy-memo-pir.sh` |
| Listener | `127.0.0.1:3002` |
| Public dashboard | `https://<MEMO_PUBLIC_URL host>/apm/` via Caddy `handle_path /apm*` |

The dashboard is unauthenticated by design: it renders only aggregate,
allowlisted metrics and host health. Caddy returns 404 for `/metrics` and
`/ready` so the raw exposition and readiness gate stay loopback-only.

## Configuration

All variables are optional; the defaults describe the memo coordinator.

| Variable | Default | Meaning |
| --- | --- | --- |
| `PIR_APM_SCRAPE_URL` | `http://127.0.0.1:8080` | Base URL of the server being watched |
| `PIR_APM_METRICS_PATH` | `/metrics` | Prometheus text exposition (must return 2xx) |
| `PIR_APM_HEALTH_PATH` | `/memo/health` | Body is shown on the dashboard; status is informational |
| `PIR_APM_READY_PATH` | `/ready` | Non-2xx for 5 continuous minutes fires the `ready` alert |
| `PIR_APM_LISTEN` | `127.0.0.1:3002` | Dashboard listener |
| `PIR_APM_METRIC_PREFIX` | `memo` | Family prefix: `<prefix>_http_*` and `<prefix>_snapshot_*` |
| `PIR_APM_ENDPOINTS` | `metadata,params,public_params,query` | `endpoint` label values to track and alert on |
| `PIR_APM_PROCESSING_ENDPOINTS` | `query` | Endpoints paged on `*_processing_duration_seconds` p99 instead of observed p99 |
| `PIR_APM_LATENCY_P99_SECONDS` | `1.0` | Default p99 budget |
| `PIR_APM_LATENCY_P99_OVERRIDES` | `query=5.0,public_params=2.0` | Per-endpoint p99 budgets, `name=seconds,...` |
| `PIR_APM_TITLE` | `Memo PIR APM` | Dashboard masthead and `<title>` |
| `PIR_APM_ENVIRONMENT` | `unknown` | Shown on the dashboard and in Slack messages |
| `PIR_APM_HOSTNAME` | OS hostname | Same |
| `PIR_APM_DATA_DIR` | `/srv/zakura/memo-data` | Mount whose disk usage is tracked |
| `PIR_APM_INTERVAL_SECONDS` | `15` | Scrape interval |
| `PIR_APM_SLACK_WEBHOOK_URL` | unset | Incoming-webhook URL. Unset: alerts are logged to the journal only |

## Alerts

Each check fires once when it starts breaching and recovers once when it stops.

| Check | Fires when |
| --- | --- |
| `scrape_failure` | 2 consecutive ticks fail to fetch or parse |
| `ready` | `/ready` is non-2xx for 300 s continuously |
| `<endpoint>_5xx` | 5xx ratio > 5 % over 5 min with at least 10 requests |
| `<endpoint>_high_latency` | p99 above the endpoint's budget with at least 20 samples |
| `disk_usage` | data-dir mount more than 90 % used |
| `memory_available` | less than 512 MiB available |

Two memo-specific expectations:

- `/memo/query` returns 503 when both query slots are busy, so a burst of
  concurrent clients can fire `query_5xx`. That is deliberate: it is the
  overload signal.
- After a deploy that re-ingests, the coordinator can take up to two hours to
  reach `serving`; `ready` fires after five minutes and recovers on its own.

## Manual use

```sh
# Local check
curl -fsS http://127.0.0.1:3002/healthz

# Confirm the Slack wiring (uses /etc/default/pir-apm)
sudo systemd-run --pipe --wait --collect \
  --property=EnvironmentFile=/etc/default/pir-apm \
  /usr/local/bin/pir-apm --send-test-alert

# Fire a synthetic alert and its recovery
sudo systemd-run --pipe --wait --collect \
  --property=EnvironmentFile=/etc/default/pir-apm \
  /usr/local/bin/pir-apm --force-alert high_latency
```

## Local development

```sh
cargo test -p pir-apm            # hermetic unit tests, part of `make test-fast`
cargo run -p memo-pir --bin memo-pir-server -- --zakura-cookie /dev/null --data-dir /tmp/memo-data
PIR_APM_ENVIRONMENT=dev cargo run -p pir-apm   # then open http://127.0.0.1:3002/
```

Without a Zakura node the coordinator's ingest loop fails and its phase reports
`failed`, but `/metrics`, `/ready` (503), and `/memo/health` are all served, which
is enough to exercise the sidecar end to end.

## Privacy

The sidecar reads only the fixed endpoint labels the coordinator emits
(`metadata`, `params`, `public_params`, `query`, `health`), the `memo_snapshot_*`
gauges, and process memory. It never sees request bodies, query contents,
client addresses, or headers, and the dashboard is rendered entirely
server-side from those aggregates.
