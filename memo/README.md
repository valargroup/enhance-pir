# Ironwood PIR fleet

`memo-pir` is the coordinator, worker, and reference client that serve the
ACTION table: one 824-byte record per Ironwood action, indexed by note
commitment tree position, over iPIR+SP. Vizor uses it to recover memos without
ever asking lightwalletd for a transaction by txid. Design and threat model are
in [`docs/vizor_tx_enhancement.md`](../docs/vizor_tx_enhancement.md); the
deployment is in [`docs/memo-pir-deploy.md`](../docs/memo-pir-deploy.md).

Build and test it with:

```bash
cargo test -p memo-pir --all-targets
cargo clippy -p memo-pir --all-targets -- -D warnings
cargo build --release -p memo-pir --bins
```

The production mode requires an archive Zakura RPC and at least two private
workers:

```bash
memo-pir-worker --listen 0.0.0.0:8091 --data-dir /srv/memo-pir/artifacts

memo-pir-server \
  --mode distributed \
  --zakura-cookie /root/.cache/zakura/.cookie \
  --data-dir /srv/zakura/memo-data \
  --worker-config /etc/memo-pir/workers.json \
  --tables action
```

`--tables` selects which PIR tables the coordinator builds and serves, by wire
name. `action` is mandatory and is the whole production scope. The server also
knows `witness`, `witness-roots`, `nf-cold`, and `nf-warm`, which the wallet's
DAG-sync pass consumes; that pass stands down by design when they are absent
from the generation manifest. Every table's journal is written regardless of
the flag, so switching a table on later needs no re-ingest.

Inspect or query the service independently:

```bash
memo-pir-cli --server https://pir.example metadata
memo-pir-cli --server https://pir.example query <ironwood-position>
memo-pir-cli --server https://pir.example dummy
```

The server will not publish until it has ingested a continuous finalized
Ironwood pool from activation. With a fresh archive snapshot, `/v1/health`
returns `503` while this catch-up and the first shard preprocessing run are in
progress.

Worker order fixes shard ownership. The worker inventory in
`/etc/memo-pir/workers.json` is append-only: add hosts at the end, never
rename, reorder, or remove an entry. Sealed artifacts in
`/srv/memo-pir/artifacts` are immutable and hash-verified on load.

The DigitalOcean configuration and the exact deployed units are under
`infra/digitalocean/production`.
