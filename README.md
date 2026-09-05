# Enhance PIR

This repository implements private Ironwood transaction enhancement. A wallet
uses an output position to recover the encrypted data needed to complete a note
without revealing the position to the server.

The active Enhance protocol has one fixed-width, 725-byte record:

| Field | Bytes | Purpose |
| --- | ---: | --- |
| `ephemeralKey` | 32 | Note key agreement |
| `encCiphertext` | 580 | Note and authenticated memo |
| `cv_net` | 32 | OVK-based outgoing recovery |
| `outCiphertext` | 80 | OVK-based outgoing recovery |
| flags | 1 | Presence of transaction-wide transparent inputs/outputs |

## Repository layout

```text
pir/enhance/                 Enhance protocol types and client
pir/transparent-spend/       outpoint-keyed spend protocol; retained, not served
server/enhance-pir-server/   coordinator, worker, ingest, and storage
server/pir-apm/              operational dashboard and alerting
ops/                         deployment tooling and infrastructure
docs/                        active design and operator documentation
docs/archive/                historical designs, not current behavior
demos/legacy-spendability/   inactive nullifier and witness experiments
```

The root Cargo workspace contains active PIR code only. The old nullifier
and witness demos are an excluded, independently buildable workspace; CI does
not build or deploy them.

## Develop

```sh
make check
cargo run --release -p enhance-pir-server --bin enhance-pir-server -- --help
cargo run --release -p enhance-pir --features cli --bin enhance-pir-cli -- --help
```

## Production performance

A five-minute, eight-worker production test completed 6,512 encrypted queries
with no errors at 21.71 requests/second. End-to-end latency was 322.6 ms p50,
483.3 ms p95, and 1.75 s p99.

At the current 32,768-row by 4,096-column configuration, each query uploads
258,056 bytes (252.0 KiB) and downloads 10,256 bytes (10.0 KiB), or 262.0 KiB
combined. At 21.71 requests/second, that is 5.60 MB/s upload plus 0.22 MB/s
download (46.6 Mbit/s combined), excluding HTTP and TLS overhead.

Live production health, request latency, throughput, fleet topology, and
capacity are available on the [Enhance PIR APM dashboard](https://enhance-pir.valargroup.dev/apm/).

The public API:

- `GET /v1/health`
- `GET /v1/enhance/init`
- `POST /v1/enhance/query`

See [architecture](docs/architecture.md) and the
[deployment runbook](docs/enhance-pir-deploy.md). No deployed wallet client
depends on the former memo/action API.

The transparent-spend PIR tables are not served. The protocol crate and the
server-side journal remain in the tree, but no worker is provisioned for them
and the coordinator does not publish them; see
[architecture](docs/architecture.md).

## Transparent activity filters

`pir/transparent-filter` implements the `zcash-transparent-basic-v1` BIP 158
profile, and `server/transparent-filter-server` builds one filter per accepted
block from Zakura and serves bounded ranges. The service runs on the coordinator
beside the archive node, bound to loopback: there is no public route and no
wallet client is enabled.

- [Range envelope format](docs/transparent_filter_envelope.md)

Filters let a wallet test its own scripts locally. They do not prove the server
built them completely; that remains a trusted-indexer assumption.
