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

The public API is intentionally narrow:

- `GET /v1/health`
- `GET /v1/enhance/generation`
- `GET /v1/enhance/params`
- `GET /v1/enhance/public-params`
- `POST /v1/enhance/query`

See [architecture](docs/architecture.md) and the
[deployment runbook](docs/enhance-pir-deploy.md). No deployed wallet client
depends on the former memo/action API.
