# Phased implementation: unified PIR deployment for DAG-sync

## Status, 2026-09-03

| Phase | State |
| --- | --- |
| 1. Widen the ACTION record | done (record v3, 824 bytes with `cmx`) |
| 2. Parameterize the coordinator by table | done (`/v1/*`, generation manifest, eight generations retained) |
| 3. WITNESS as a sharded table | done, not served in production |
| 4. NULLIFIER cold/warm | done, not served in production |
| 5. Production deployment shape | **replaced**: the memo fleet is the production shape; see `pir_deployment_architecture.md` §6 and `memo-pir-deploy.md` |
| 6. Query envelope and protocol version | done; packing-key batch spike deferred |
| 7. Transparent-address table | next; design in `pir_deployment_architecture.md` §7.1 |

The shipped scope is memo retrieval only. The coordinator serves the tables named by
`--tables` (production: `action`), journals every table regardless, and the wallet's
DAG-sync pass stands down when a table is absent. The three-pool fleet in the original
Phase 5 is recorded as a growth path and is not planned. The plan text below is kept as
written for the record; where it says "retain two generations" the implementation retains
eight.

## Context

[`pir_deployment_architecture.md`](pir_deployment_architecture.md) proposes one deployable system from the three PIR services: an ACTION table (memo PIR with a widened record), a full-pool WITNESS table with tier A/B rows on the same row-shard framework, a NULLIFIER table split cold/warm, one generation manifest, one worker pool per table, and deploy tooling generalized from `vote-nullifier-pir`. This plan turns that into ordered, shippable phases across three repos:

- Server: `~/projects/spendability-pir` (`main`, clean).
- Wallet library: `~/projects/wallet-libraries` (branch `feat/ironwood-memo-pir`, PR #13 open, HEAD `e8447921…`). Crates: `zakura/pir-memo` (client, 707 LOC), `zcash_client_backend/src/data_api/memo_pir.rs` (traits + `decrypt_and_store_ironwood_memo`), `zcash_client_sqlite/src/wallet/memo_pir.rs` (queue table `ironwood_memo_retrieval_queue`).
- App: `~/projects-copy/vizor-wallet` (branch `roman/ironwood-memo-pir`, one commit). `rust/src/wallet/sync_engine/memo_pir.rs` (371 LOC) schedules queries, validates the anchor via `memo_pir_snapshot_status`, routes through Tor or `DirectRouteConnector`; `enhance.rs` carries the `suppress_tx_enhancement` gate.

Codebase facts that fix the order:
- The memo coordinator is welded to memo constants (`coordinator.rs` imports `RECORD_BYTES`, `ROW_BYTES`, `SHARD_ROWS` from `types.rs`); `ipir.rs::global_parameters` already takes only row counts. One generation retained. No `test-memo` CI job; `memo/memo-pir/tests/` is empty.
- The wallet client crate mirrors the same constants (`zakura/pir-memo/src/types.rs:56`), validates `Coverage::Full`, the setup seed, geometry, anchor, `params_for_simplepir`, and public-params digest. It is transport-neutral (`MemoPirSession`) with an optional reqwest client. Endpoints are `/memo/*`; generation is a metadata field echoed in the body.
- The wallet DB already stores everything a `(position, nf)` pass needs in `ironwood_received_notes` (`rho`, `nf`, `commitment_tree_position`, `recipient_key_scope`, `witness_stabilized`), but no query returns position and nullifier together. Witnesses are always recomputed from the local shard store; there is no API accepting an external Merkle path.
- Nullifier server geometry is compile-time (`NUM_BUCKETS`, `Bucket { entries: [_; BUCKET_CAPACITY] }`); `TARGET_SIZE` eviction is called from `spend-server` and `combined-server`. Witness window logic is in `witness-types` (`physical_row_index`, `L0_MAX_SHARDS`), `commitment-tree-db` (`window_start_shard`), and `witness-client` (`PositionOutsideWindow`, `reconstruct.rs:34`).
- `ipir-sp` pins: memo and wallet client on `e875404…`, nullifier/witness on `2bc1075…`.
- `infra/digitalocean/memo-poc` (now `production`) keeps its Terraform state untracked; `.gitignore` covers `tfstate`, `tfplan`, and populated `tfvars`. (An earlier revision of this plan said the state was committed; it was not.)

Ordering principle: the one irreversible-cost change is the ACTION record (widening later rebuilds every sealed shard) and the pool is 136K positions today, so it ships first, server and wallet together. Everything after is refactor-then-extend on the memo framework, with the wallet phase that consumes each server phase listed beside it.

---

## Phase 1 — Widen the ACTION record

**Server** (`spendability-pir`)
- `memo/memo-pir/src/types.rs`: `RECORD_BYTES = 792`, `ROW_BYTES = 6336`; `MemoRecord` → `ActionRecord::from_parts(nf, ephemeral_key, ciphertext, cv_net, out_ciphertext, txid, height)`; bump `SCHEMA_VERSION`; new `parameter_id`.
- `bin/server.rs::ingest()` + `zakura.rs`: extract the added fields from the raw block; keep the per-block `trees.ironwood.size` cross-check.
- `store.rs`: bump `STORE_VERSION`, refuse v1 journals (POC re-ingests from activation). `ipir.rs`: bump `ARTIFACT_VERSION`, update `RowPlaintextIter`; extend `memo_rows_use_two_ipir_instances` to assert the instance count at 6,336 bytes and write the answer into the doc §3.1.
- Tests: existing `types.rs`/`store.rs` tests; new `memo/memo-pir/tests/record_layout.rs` (fixture raw action → every field offset).
- Redeploy the POC with a full rebuild via `scripts/deploy-memo-pir.sh`.

**Wallet library** (`wallet-libraries`, on top of PR #13)
- `zakura/pir-memo/src/types.rs`: same constants and `parameter_id`; `MemoPirRow::record` returns the 792-byte record; accessors for the new fields.
- `zcash_client_backend/src/data_api/memo_pir.rs`: unchanged. `IronwoodMemoRecord` is the backend's memo-authentication view (ephemeral key and ciphertext only), so it stays narrow; the DAG-sync fields enter the backend in Phase 4 where they are first consumed.

**App** (`vizor-wallet`): bump the `wallet-libraries` rev in `rust/Cargo.toml` and `[patch.crates-io]`; re-run `deployed_endpoint_accepts_and_decodes_a_private_query` against the redeployed POC; `docs/memo_pir_demo.md` rev line.

Gate: POC serves the new record; boundary-position queries byte-match the journal; Vizor memo demo completes memos end to end.

## Phase 2 — Parameterize the coordinator by table

Goal: one coordinator process hosts several tables (ACTION, WITNESS, NF-COLD, NF-WARM), each with its own layout, worker pool, and shards, under one generation. Today the coordinator, journal, wire frames, and metadata are welded to the memo constants. Only ACTION lands in this phase.

**Server**
- `types.rs`: `DatabaseLayout { record_bytes, records_per_row, shard_rows }`, `DatabaseId` (`Action`, `Witness`, `NfCold`, `NfWarm`), `WorkerPool` (ordered URL list per table); `worker_index_for_shard(shard_id, &WorkerPool)` replaces the `SHARDS_PER_WORKER` list.
- `store.rs`: `MemoStore` → `RecordJournal` parameterized by layout, one path per table.
- `coordinator.rs`: `CoordinatorState` holds a map `DatabaseId → TableState`; `answer_query`/`publish_from_store` take a `DatabaseId`; `LiveSnapshot` → `GenerationSnapshot`; **retain two generations** (`VecDeque` of two behind the swap; a query naming either is served).
- Metadata: `MemoSnapshotMetadata` → `GenerationManifest { network, anchor_height, anchor_hash, tree_size, cold_checkpoint_height, envelope, databases: BTreeMap<DatabaseId, TableManifest> }`, reusing `ShardDescriptor`. Lives in `shared/pir-types`; memo-pir now depends on `pir-types` and drops its duplicated `CONFIRMATIONS`, `POOL`, activation height, epoch helpers.
- `wire.rs`: `database_id` in the frames (`MPQ2`/`MPH2`); `worker.rs`: shard dirs `db-{id}/shard-{id:08}`.
- Routes: `/v1/generation`, `/v1/{db}/{params,public-params,query}`, `/v1/health`; `/memo/*` kept as ACTION aliases until the app moves.
- Move nullifier/witness `ipir-sp` pins to `e875404…`; re-run `test-ipir`.
- Tests: `memo/memo-pir/tests/sharded_vs_monolithic.rs` (in-process workers vs monolithic evaluation at memo layout; the memo doc §11 open gate); two-generation retention; wire round-trip with table id. CI: `test-memo` job in `ci.yml` (paths `memo/**`, `shared/**`).

**Wallet library**
- `zakura/pir-memo` → rename crate to `zakura-pir-client`; `MemoPirSession` → `PirSession` built from a `GenerationManifest` + per-table params, keyed by `DatabaseId`; `prepare_row(db, row)`, `prepare_dummy(db)`, `decode(db, …)`; validation list unchanged but applied per table. `HttpMemoPirClient` → `HttpPirClient` with `/v1/*` paths, `/memo/*` fallback removed once the app is on `/v1`.
- No backend changes.

**App**: `memo_pir.rs` uses the new session on `/v1/action/*` and `/v1/generation`; anchor check now reads the manifest; nothing else changes.

Gate: POC answers identically on `/v1/action/query` and `/memo/query`; transcript test green; Vizor demo green on `/v1`.

## Phase 3 — WITNESS as a sharded table

**Server**
- Ingest writes a `commitments` journal (32 B cmx per position; layout `record_bytes = 32`, `records_per_row = 256`) and a `subshard_roots` journal (tier A rows: 256 sub-shard roots per shard), computing roots with `commitment-tree-db`'s `subshard_roots`/`shard_roots` (`MerkleHashOrchard`) and its warm-cache idea for the frontier. One `Witness` table: tier A rows `[0, S)`, tier B rows `[S, S + 256·S)`, `S = logical_shards` (power of two, prefix-stable). Row-offset rule documented in `types.rs`.
- `GET /v1/witness/cap` (all shard roots, `tree_size`, anchor) and `GET /v1/witness/frontier?from=&to=` from a ring buffer of per-block rightmost paths (`plans/future/frontier_witness_update_design`, ~2,000 blocks) held by the coordinator.
- Retire `witness/witness-server` and the `combined-server` `witness` feature; keep `commitment-tree-db` as the hashing library; delete `L0_MAX_SHARDS`, `window_start_shard`, `PositionOutsideWindow`.
- Tests: port `commitment-tree-db/tests/tree_correctness.rs` to tier A/B rows; `memo/memo-pir/tests/witness_path.rs` (A + B + cap → path verifies against the block root from Zakura); frontier update test.

**Wallet library**
- `zakura-pir-client`: `fetch_witness(position) → MerklePath` = tier A query + tier B query + cap reconstruction (port `witness-client/src/reconstruct.rs` minus window arithmetic); `update_witness(path, frontier_paths)`; verify the resulting root against the cap root before returning.
- Backend: new `data_api::pir_witness` with `PirWitnessRead::notes_needing_witness()` (unspent Ironwood notes with `witness_stabilized = 0`, returning position) and `PirWitnessWrite::put_external_witness(position, path, anchor_height)`. Storage: a new `ironwood_pir_witnesses` table (note id, anchor height, 32 siblings) rather than forcing the path into shardtree; `witness_stabilized` semantics extended so a stored external witness verified against the local block commitment also counts as spendable (touch point `wallet.rs:2752`). Send path reads the external witness when the shardtree one is unavailable (`commitment_tree.rs:1480` region).
- Feature flag `zakura-pir-witness` in backend, sqlite, `wallet-lib`.

**App**: `sync_engine/pir_witness.rs` mirroring `memo_pir.rs` (same anchor gate, same routed transport); `send.rs::orchard_witnesses` falls back to the external witness for Ironwood inputs; balance summary treats externally witnessed notes as spendable.

Gate: a note received into a sealed shard becomes spendable before the local shard completes; the send E2E on regtest passes with an external witness; no window symbols remain.

## Phase 4 — NULLIFIER cold/warm on the framework

**Server**
- `spend-types` / `hashtable-pir`: runtime `num_buckets` (`HashTable::with_buckets`), `hash_to_bucket(nf, num_buckets)`; remove `evict_oldest_block`, `evict_to_target`, `TARGET_SIZE`.
- Ingest writes a `nullifiers` journal (`nf ‖ SpendMeta`, 41 B) from raw blocks, `first_output_position` from the running tree size (port `nf-ingest::extract_nullifiers_with_meta`).
- Tables: `NfCold` (layout `record_bytes = BUCKET_BYTES`, `records_per_row = 1`, sharded by bucket range over the nf pool, rebuilt when `cold_checkpoint_height` advances, daily); `NfWarm` (small fixed table, replicated on every nf worker, rebuilt per generation). Both in the manifest with `num_buckets`.
- Retire `nullifier/spend-server`, `nf-ingest` follow loop, and `combined-server` entirely; delete `infra/digitalocean/main.tf`.
- Tests: port `spend-server/tests/server_test.rs` cases (insert, reorg rollback, checkpoint boundary); cold rebuild timing at 1M synthetic nullifiers recorded in the doc §3.3.

**Wallet library**
- `zakura-pir-client`: `check_spent(nf) → Option<SpendMeta>` as a fixed COLD + WARM pair (port `spend-client::scan_bucket_for_nf`).
- Backend: `data_api::pir_spend` with `notes_for_spend_check()` returning `(note id, position, nf)` (new join on `ironwood_received_notes`; today `get_nullifiers` returns only `(account, nf)`), and `put_spend_observation(note id, SpendMeta)` that records the spend in `ironwood_received_note_spends` with a provisional tx row keyed by `spend_height`/`first_output_position` until ACTION rows fill in the txid. `DagSyncEligible` classification (fail closed) alongside the memo queue reconciliation in `wallet/orchard.rs`.
- Change discovery: `discover_change(action_rows)` trial-decrypts ACTION records with the IVK using the record's `nf` as `rho` (reuse the `FullOutput` decryption path in `data_api/memo_pir.rs:155-230`), inserts discovered notes through the existing received-note upsert, and enqueues them for the next pass.

**App**: `sync_engine/dag_sync.rs` runs the pass at sync start (spend check → change discovery → witness) before the compact-scan loop, plus the tail scan `[anchor+1, tip]` which the existing scan loop already covers.

Gate: a wallet restored from seed at the Ironwood activation birthday shows correct spendable balance and change chain before compact scanning reaches the tip, on regtest and against the POC.

## Phase 5 — Production deployment shape

> Replaced. Production is the memo fleet renamed (`infra/digitalocean/production`, GitHub Environment `production`, `deploy-pir-fleet.yml`), serving ACTION only. The items below are the growth path in `pir_deployment_architecture.md` §6.4.

**Server only**
- Split ingest into `memo/memo-pir/src/bin/ingest.rs` (Zakura poll loop, journals, frontier push via `/prepare`); the coordinator reads published generations.
- Spaces publisher in ingest: sealed shard artifacts + `manifest.json` (written last, `no-cache`) to `s3://<bucket>/pir/<network>/<generation>/`; worker `load` restores missing sealed shards from Spaces with digest check. Pattern: `vote-nullifier-pir/.github/workflows/publish-snapshot.yml`.
- Terraform `infra/digitalocean/{staging,production}`: pools as `for_each` droplet sets (`action`, `witness`, `nf`), 2 coordinators, 1 ingest with the archive volume; remote state; remove committed `terraform.tfstate` and `.tfplan` from `memo-poc`, gitignore them.
- Workflows: `deploy.yml` (inputs `target_environment`, `role`, `pool`, `release_tag`), `restart.yml` (rolling, gate `/ready` + `served_generation >= expected_generation`), `publish-generation.yml`; deploy script diffs each pool's ordered list against the manifest and **refuses reorders**; `release.yml` adds `ingest`/`worker` binaries.
- Observability: sidecar scrape of `/metrics`, Sentry generation-staleness watchdog, aggregate metrics only. Runbooks under `docs/runbooks/`; `ai-runbook` rules/skills updated in a separate PR.

Gate: staging serves all four tables; drills: worker restart (digests unchanged), add an action worker (no sealed shard moves), reorder refused, coordinator failover under load.

## Phase 6 — Query envelope and protocol version

**Server**: `QueryEnvelope { k_nf, k_act, k_wit, protocol_version }` in the manifest (constants agreed once, e.g. 8 / 4 / 4); coordinator rejects bodies from unknown protocol versions. Time-boxed spike in `ipir-sp` for a batch API amortising packing keys; its result decides whether ACTION rows widen to 32 records before 1M positions.

**Wallet library**: `DagSyncSession` in `zakura-pir-client` owns the queues, dummy padding from `OsRng`, overflow deferral, the fixed pair counts, and adjacent-row handling for `action_count > 8`. Rejects unknown `protocol_version`.

**App**: `dag_sync.rs` and `memo_pir.rs` issue exactly the envelope per pass; remove `/memo/*` usage; the per-row scheduling in `memo_pir.rs:94-120` moves into the session. Retire the `/memo/*` aliases server-side afterwards.

Gate: transcript tests showing zero / one / many pending notes produce identical request sequences per pass (memo doc §11 privacy gate), in both the library and the app.

---

## Dependency summary

```
P1 server ──► P1 wallet-lib ──► P1 app          (ship together; record change)
P2 server ──► P2 wallet-lib ──► P2 app          (routes + session)
P3 server ──► P3 wallet-lib ──► P3 app          (witness)      ┐ independent of each other,
P4 server ──► P4 wallet-lib ──► P4 app          (nullifier)    ┘ both need P2
P5 server  (needs P3 + P4 to retire combined-server)
P6 server ──► P6 wallet-lib ──► P6 app          (envelope; needs P3 + P4 client paths)
```

## Verification

- Server: `cargo test -p memo-pir`, workspace `cargo test --workspace --all-features --release` (`test-ypir`, `test-ipir`, new `test-memo`); every phase re-runs the boundary-position byte-match from the memo doc §8.2 on the POC (P1–P4) or staging (P5–P6).
- Wallet library: `cargo test -p zakura-pir-client` in its own invocation (feature-unification rule in `verify.yml`), `cargo test -p zcash_client_sqlite --features zakura-pir-memo,zakura-pir-witness,zakura-pir-spend`.
- App: `cd rust && cargo test wallet::sync_engine::memo_pir`, `…::dag_sync`, the ignored live test against the deployed endpoint, and the regtest send E2E (`scripts/e2e/flutter-macos-regtest-import-sync.sh`) once P3 lands.
- Docs updated in the same PR as the code (`docs/vizor_tx_enhancement.md` constants, `docs/pir_deployment_architecture.md` resolved questions, `docs/memo_pir_demo.md` revs). No phase merges with `TARGET_SIZE`, `L0_MAX_SHARDS`, or `PositionOutsideWindow` referenced after the phase that removes them.

## Phase 7 — Transparent-address table (next)

Design: `pir_deployment_architecture.md` §7.1. Server: a `t-utxo` journal from the raw
blocks ingest already reads, cold/warm tables on the `NullifierTables` framework keyed by
script hash with a capped UTXO list and a `used` flag, served under `--tables`. Wallet
library: a fixed cold+warm pair per address with dummies, replacing the
`TransactionsInvolvingAddress` stream and `download_transparent_outputs` in Vizor. Gate:
a restored wallet with transparent receivers shows its transparent balance and completes
gap-limit discovery with no address ever sent to lightwalletd.
