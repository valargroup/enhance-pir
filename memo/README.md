# Ironwood memo PIR POC

`memo-pir` is a standalone iPIR-SP proof of concept. It is deliberately not
wired into Vizor.

Build and test it with:

```bash
cargo test -p memo-pir --all-targets
cargo clippy -p memo-pir --all-targets -- -D warnings
cargo build --release -p memo-pir --bins
```

The production-shaped mode requires an archive Zakura RPC and at least two
private workers:

```bash
memo-pir-worker --listen 0.0.0.0:8091 --data-dir /srv/memo-pir/artifacts

memo-pir-server \
  --mode distributed \
  --zakura-cookie /root/.cache/zakura/.cookie \
  --data-dir /srv/zakura/memo-data \
  --worker-url http://worker-1:8091 \
  --worker-url http://worker-2:8091
```

Inspect or query the service independently:

```bash
memo-pir-cli --server https://memo-pir.example metadata
memo-pir-cli --server https://memo-pir.example query <ironwood-position>
memo-pir-cli --server https://memo-pir.example dummy
```

The server will not publish until it has ingested a continuous finalized
Ironwood pool from activation. With a fresh archive snapshot, `/memo/health`
returns `503` while this catch-up and the first shard preprocessing run are in
progress.

Each worker owns two fixed shard IDs. Preserve URL order when adding hosts:
the first URL owns shards 0–1, the second 2–3, and so on. Sealed artifacts in
`/srv/memo-pir/artifacts` are immutable and hash-verified on load. Changing URL
order changes ownership and is therefore an operator error.

The DigitalOcean POC configuration and exact deployed units are under
`infra/digitalocean/memo-poc`.
