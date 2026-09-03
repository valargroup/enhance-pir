# Spendability PIR

Single-server Private Information Retrieval (PIR) for Zcash wallets on the
**Ironwood** shielded pool (NU6.3, mainnet activation height **3,428,143**).

## What ships

The product is the **ACTION table** served by the `memo/` fleet. It removes one
concrete privacy leak from Vizor: after compact-block scanning finds a note, the
wallet no longer asks lightwalletd for that transaction by txid to recover the
memo. Instead it issues exactly one real-or-dummy PIR query per completed scan
batch against a position-indexed table of every Ironwood action, and the server
learns neither the transaction, nor the position, nor whether the batch found
anything. The record (824 bytes: nullifier, ephemeral key, full ciphertext,
commitment, value commitment, out-ciphertext, txid, height) is wider than memo
recovery needs so that later scopes do not force a rebuild of sealed shards.

- [`docs/vizor_tx_enhancement.md`](docs/vizor_tx_enhancement.md) — design, threat
  model, database layout, wallet contract
- [`docs/memo-pir-deploy.md`](docs/memo-pir-deploy.md) — production fleet, GitHub
  Environment, rollout and rollback
- [`docs/pir_deployment_architecture.md`](docs/pir_deployment_architecture.md) —
  scope status, the DAG-sync tables that are built but not a supported path, the
  growth path, and the next milestone (transparent addresses)
- [`memo/README.md`](memo/README.md) — build, run, and operator notes

## Workspace

```
spendability-pir/
├── memo/memo-pir/            # coordinator, workers, ingest, reference client (the product)
├── deploy/pir-apm/           # monitoring sidecar for the coordinator
├── shared/
│   ├── pir-types/            # DatabaseId, layouts, generation manifest, CONFIRMATION_DEPTH
│   └── chain-ingest/         # lightwalletd client (legacy servers only)
├── infra/digitalocean/       # production Terraform root and deployed unit files
├── scripts/                  # deploy-memo-pir.sh, driven by the fleet workflow
│
│   # legacy, not deployed — kept as libraries until removed
├── nullifier/                # bucketed nullifier PIR (spend-server, spend-client, ...)
├── witness/                  # windowed witness PIR; memo-pir uses commitment-tree-db
├── combined-server/          # the retired single-host binary
└── proto/
```

The `nullifier/` and `witness/` subsystems were the first two PIR services and
their wallet integration is described in
[`docs/pir_wallet_integration.md`](docs/pir_wallet_integration.md). Their
successors (full-pool witness and cold/warm nullifier tables) exist inside
`memo-pir` for the wallet's DAG-sync pass, which is implemented but outside the
shipped scope. See the deployment architecture document for the reasoning.

## Build and test

```bash
cargo build --release -p memo-pir --bins    # memo-pir-server, memo-pir-worker, memo-pir-cli
make test-fast                              # hermetic: no network, no PIR crypto (~12s warm)
make check                                  # cargo check --workspace --all-targets
make lint                                   # clippy, all targets and features (matches CI)
```

Tests are split into three tiers so the default command is always safe to run;
see [CLAUDE.md](CLAUDE.md). `make test-slow` and `make test-bench` exercise the
legacy servers against mainnet and are opt-in.

## License

MIT
