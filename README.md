# Enhance PIR

This repository implements private Ironwood transaction enhancement. A wallet
uses an output position to recover the encrypted data needed to complete a note
without revealing the position to the server.

The active protocol has one fixed-width, 724-byte record:

| Field | Bytes | Purpose |
| --- | ---: | --- |
| `ephemeralKey` | 32 | Note key agreement |
| `encCiphertext` | 580 | Note and authenticated memo |
| `cv_net` | 32 | OVK-based outgoing recovery |
| `outCiphertext` | 80 | OVK-based outgoing recovery |

## Repository layout

```text
pir/enhance/                 public protocol types and client
server/enhance-pir-server/   coordinator, worker, ingest, and storage
server/pir-apm/              operational dashboard and alerting
ops/                         deployment tooling and infrastructure
docs/                        active design and operator documentation
docs/archive/                historical designs, not current behavior
demos/legacy-spendability/   inactive nullifier and witness experiments
```

The root Cargo workspace contains active Enhance code only. The old nullifier
and witness demos are an excluded, independently buildable workspace; CI does
not build or deploy them.

## Develop

```sh
make check
cargo run --release -p enhance-pir-server --bin enhance-pir-server -- --help
cargo run --release -p enhance-pir --features cli --bin enhance-pir-cli -- --help
```

## Load test

The standalone `enhance-pir-load-test` crate drives real encrypted queries
through the public origin without adding benchmark dependencies to the client
crate. `make load-test` uses closed-loop concurrency: each worker selects a
random advertised Ironwood position and starts its next request as soon as the
previous request completes.

`LOAD_TEST_URL` is deliberately required so a production service is not loaded
by accident:

```sh
make load-test \
  LOAD_TEST_URL=https://enhance-pir.valargroup.dev \
  LOAD_TEST_DURATION=5m \
  LOAD_TEST_PARALLELISM=8 \
  LOAD_TEST_WARMUP=30s \
  LOAD_TEST_SEED=42
```

The defaults are a 60-second measured phase, eight workers, a 10-second
unmeasured warmup, a 1% maximum error rate, and a JSON report at
`load-test-summary.json`. Set `LOAD_TEST_JSON=` to disable the JSON file.
`LOAD_TEST_MAX_ERROR_RATE` changes the failure threshold, and
`LOAD_TEST_SLO_P99_MS` optionally fails the command when end-to-end p99 exceeds
the given number of milliseconds.

The console and JSON reports include achieved requests per second, success and
error counts, classified 429/503/timeout failures, and
p50/p90/p95/p99/p99.9/max latency for the full operation, query preparation,
the HTTP exchange through the proxy and coordinator, and response decoding.

The public API is intentionally narrow:

- `GET /v1/health`
- `GET /v1/enhance/generation`
- `GET /v1/enhance/params`
- `GET /v1/enhance/public-params`
- `POST /v1/enhance/query`

See [architecture](docs/architecture.md) and the
[deployment runbook](docs/enhance-pir-deploy.md). No deployed wallet client
depends on the former memo/action API.
