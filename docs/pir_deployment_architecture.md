# Unified PIR deployment for DAG-sync

Status: design proposal, 2026-09-02. Nothing here is implemented. It builds on the deployed
memo PIR proof of concept described in [`vizor_tx_enhancement.md`](vizor_tx_enhancement.md)
and proposes how the nullifier, witness and transaction-enhancement services become one
deployable system that a DAG-sync wallet can use.

---

## 1. Summary

Three PIR services exist in this repository. They were built separately and cover the
chain differently:

| Service | Index key | Coverage today | Deployment today |
| --- | --- | --- | --- |
| Nullifier PIR (`nullifier/spend-server`) | hash bucket of the nullifier | rolling `TARGET_SIZE = 1,000,000` nullifiers (~289 days), whole-block eviction | one combined `pir-server` host, lightwalletd ingest |
| Witness PIR (`witness/witness-server`) | commitment-tree position, row = 256 leaves | 32-shard sliding window (~2.1 M notes); `NoteOutsideWindow` beyond it | same combined host |
| Memo PIR (`memo/memo-pir`) | commitment-tree position, row = 8 actions | full Ironwood pool from activation, row-sharded coordinator and workers, sealed shards | separate three-host Terraform POC, Zakura archive ingest |

A DAG-sync wallet follows its own transaction graph instead of scanning: for every note it
knows, ask whether the note is spent; if it is, read the spending transaction's outputs and
trial-decrypt them to find change; recurse; and fetch witnesses for whatever is unspent.
None of those requests may carry a txid, position, nullifier or block height in plaintext.

That works only if the three services (1) cover the full Ironwood pool, (2) answer against
one consistent snapshot, and (3) share ingestion, sharding, generation discipline and
deployment. The memo POC's row-sharded design is the base. The witness service is reshaped
onto it. The nullifier service cannot be, for a structural reason explained in §3.3, and
gets a cold/warm split instead.

Two rules from the memo document carry over unchanged: no record is overloaded beyond what
the wallet actually needs, and the number of requests a wallet makes never depends on what
it found.

Assumptions:

- "Nullifier PIR" means the wallet spendability service in this repository, not the
  Shielded Vote `nf-server` in `vote-nullifier-pir`. That fleet's operational tooling is
  reused (§6); its dataset is not merged (§7).
- Ironwood pool only, as all three services already are.
- The wallet keeps linear compact-block scanning for discovering payments from third
  parties. DAG-sync cannot discover those; it is a fast path for spendability, change and
  history, not a replacement for discovery (§3.4).

---

## 2. What a DAG-sync pass asks

For each known note at tree position `p` with nullifier `nf`:

```text
1. NF-COLD(nf), NF-WARM(nf)
     -> unspent
      | SpendMeta { spend_height, first_output_position, action_count }

2. if spent:
     ACTION[rows covering first_output_position .. first_output_position + action_count]
     -> trial-decrypt every action with the wallet's IVK
     -> change notes at positions p'
     -> recurse from step 1 with p'

3. if unspent:
     WITNESS tier A[shard(p)] + WITNESS tier B[subshard(p)] + public cap
     -> Merkle path to the anchor, note is spendable

4. tail [anchor + 1 .. tip]:
     client downloads and scans compact blocks linearly
     (own nullifiers and own outputs; ten to twenty blocks)
```

Four logical databases, one generation. The generation is the anchor height, and every
query in one pass is made against the same generation.

---

## 3. Data model

### 3.1 ACTION database — the transaction-enhancement index

This is the memo PIR database under a name that reflects what it must hold. It stays
position-indexed with sealed shards, and the coordinator and worker algebra in
`vizor_tx_enhancement.md` §5–§6 is unchanged.

The record must change, and it must change **before** any wallet traffic exists: widening
a record later invalidates every sealed shard artifact (§10.1 of the memo document), which
is an append-only rebuild of the whole pool.

```text
nf[32] ‖ ephemeralKey[32] ‖ encCiphertext[580] ‖ cv_net[32] ‖ outCiphertext[80] ‖ txid[32] ‖ height[4]
= 792 bytes            8 per row = 6,336-byte row            row = p >> 3, slot = p & 7
```

Why each addition is required, named by the DAG-sync step that fails without it:

| Field | Bytes | Needed by |
| --- | ---: | --- |
| `nf` (the action's spent nullifier, which is `rho` of the new note) | 32 | Step 2. Ironwood note decryption takes `rho` from the action. The 612-byte memo record cannot decrypt an unknown note at all; it can only complete a memo for a note that compact scanning already found. |
| `cv_net`, `outCiphertext` | 112 | Outgoing recovery under the OVK after a seed restore. Without them sent history is unrecoverable except by requesting transactions by txid. The memo document preferred a second column; one record is better here because DAG-sync reads the row anyway, and a second column is a second query in the fixed schedule of §5. |
| `txid`, `height` | 36 | Transaction history and confirmation depth without any lightwalletd call. Today change discovery downloads the compact block at `spend_height` from lightwalletd (`nullifier/README.md`, "Change Note Discovery"), a block-level leak that must go. |

`cmx` is deliberately omitted: it is recomputed from the decrypted note, and witness tier B
returns the leaf for verification.

The `DecryptionLeaf { nf, ephemeral_key, ciphertext[52] }` record from
`plans/wip/decryption_pir_implementation_4a3d65f2.plan.md` is subsumed by this record and
that plan should be closed against this document.

Geometry, confirmed by `params_for_simplepir` (test `action_rows_use_two_ipir_instances`):
with `d = 2048` and `p = 2^14` one iPIR instance carries 28,672 plaintext bits, so the
6,336-byte row (50,688 bits) still fits `instances = 2`. `db_cols`, the request, and the
10 KiB response are unchanged from the 612-byte layout. The memo document's earlier §10.1
estimate that a 724-byte record would need three instances was wrong.

### 3.2 WITNESS database — full pool, two PIR tiers and a broadcast cap

Leaf rows stay as they are: 256 leaves × 32 bytes = 8 KiB, row index `p >> 8`. The sliding
window goes away. Leaf rows are position-indexed and append-only exactly like ACTION rows,
so the same shard placement and sealed/frontier split apply, at 8,192 rows per shard
(2,097,152 positions, 64 MiB).

The current broadcast of "sub-shard roots for the active window" does not survive the
window's removal: at 50 M positions it would be 195 K roots, 6.25 MB. Replace it with:

| Levels | Served as | Row | Size at 50 M positions |
| --- | --- | --- | ---: |
| 0 → 16 (cap) | public broadcast | — | 763 shard roots, 24 KiB |
| 16 → 24 (tier A) | PIR | one row per shard: its 256 sub-shard roots, 8 KiB | 763 rows |
| 24 → 32 (tier B) | PIR | one row per sub-shard: 256 leaves, 8 KiB | 195,313 rows |

Tier A and tier B rows have the same width, so they live in one physical database: rows
`[0, S)` are tier A, rows `[S, S + 256·S)` are tier B, `S` being the shard count. Two queries
against one database, one set of shard artifacts. `witness/README.md` rejected three tiers
only because the windowed middle tier had about ten rows; a full-pool middle tier does not.

The consequence that matters most: **a note in a sealed shard needs its witness fetched
once, ever.** Levels 0–16 come from the broadcast cap and levels 16–32 are immutable once
the shard seals. Only notes in the frontier shard re-query, and
`plans/future/frontier_witness_update_design_1bab3c5a.plan.md` (broadcast the ~1 KB
rightmost path per block, let clients update witnesses locally) removes even that. Adopt
it. It also removes the per-block `engine.setup()` over the whole database, because only
the frontier shard is rebuilt.

### 3.3 NULLIFIER database — full history, cold and warm

This database **cannot be position-indexed**. A nullifier reveals nothing about the position
of the note it spends, and the server holds no viewing keys, so membership must stay keyed
by `hash(nf)`. New nullifiers land in random buckets, so every shard mutates every block.
There are no sealed shards here, and the memo-style "only the frontier churns" property does
not exist. The design accepts that and controls when the full rebuild happens:

- **NF-COLD**: every nullifier up to a daily checkpoint height. The existing bucketed table
  (`spend-types`: 41-byte entries of `nf ‖ SpendMeta`, `BUCKET_CAPACITY = 112`,
  `hash_to_bucket = u32_le(nf[0..4]) % NUM_BUCKETS`). `NUM_BUCKETS` is a power of two chosen
  so load stays at or below ~55% at the checkpoint. Rebuilt and re-preprocessed once per
  day. Row-sharded by bucket range across the nullifier worker pool; the coordinator slices
  the global query by row exactly as for ACTION, with row = bucket.
- **NF-WARM**: nullifiers since the checkpoint, at most about one day (4–35 K entries at
  today's volume; size it for ten times that). A small fixed-geometry table rebuilt every
  generation. Today's server rebuilds a 72 MB table in ~2.3 s, so this is sub-second.
- **Tail**: `[anchor + 1, tip]` is scanned client-side from compact blocks. This answers the
  memo document's objection that spentness needs fresher data than a ten-confirmation
  snapshot. Freshness comes from the tail scan, not from a faster PIR cadence.
- `TARGET_SIZE` eviction is removed. Growth is a doubling of `NUM_BUCKETS` at a daily
  rebuild, which is a full rebuild anyway. At 50 M nullifiers that is 1.9 GiB of entries, about 3.5 GiB
  across the pool at ~55% load.

The client always issues one COLD and one WARM query per nullifier, so the split leaks
nothing about which table answered.

Integrity caveat, to be stated wherever the wallet consumes this: tree roots are committed
on chain; the nullifier set is not. Non-membership is trusted, not verified. A dishonest
server can make the wallet believe a spent note is spendable (the send then fails at
broadcast) or hide a spend. That is the same trust the existing spendability gate already
places in the server.

### 3.4 Generation and anchor

One manifest per generation:

```text
{ schema_version, network, anchor_height, anchor_hash, ironwood_tree_size,
  cold_checkpoint_height,
  databases: {
    action:  { parameter_id, public_params_epoch, logical_rows, shards: [{id, rows_sha256, sealed, worker}] },
    witness: { ... },
    nf_cold: { ..., num_buckets },
    nf_warm: { ..., num_buckets }
  } }
```

A pass pins one generation for all of its queries. Coordinators retain two generations so an
in-flight pass finishes on the generation it started on. Publication is gated at
`tip − CONFIRMATION_DEPTH` for every database; `CONFIRMATION_DEPTH = 10` is already shared
through `shared/pir-types`.

The wallet must bind `anchor_hash` to a block it trusts before it sends a query. Because
incoming-note discovery still requires compact-block scanning, the wallet has the compact
block chain and verifies the anchor hash against it, then verifies the witness cap root
against the block's tree commitment. That is the rule the memo demo already applies. A
wallet with no scan at all would need header sync; that is out of scope here.

---

## 4. Sharding

Sharding the witness service is the right call, and it is the same mechanism the memo POC
already has rather than a new one:

- ACTION and WITNESS are both position-indexed and append-only. `worker_index_for_shard`,
  the sealed/frontier split, and the prefix-stable public setup apply verbatim.
- The reason is coverage, not throughput. The 32-shard window makes old notes fall back to
  scanning, which a DAG-sync wallet cannot do.
- The nullifier database shards by bucket range and has no sealed shards (§3.3). Its cost
  is a daily full re-preprocess, not a per-block frontier rebuild.

Sharding does **not** address the request-size growth in `vizor_tx_enhancement.md` §8.1.
The SimplePIR first-dimension query grows linearly in `logical_rows`:

| Database at 50 M positions | Rows | Request |
| --- | ---: | ---: |
| WITNESS (256 positions per row) | ~197 K | ~1.1 MiB |
| ACTION (8 positions per row) | ~6.3 M | ~36 MiB |

ACTION needs one of the following before the pool passes about one million positions, in
order of preference:

1. Amortise the 84 KiB packing keys across a batch of queries: one key set per pass, not
   per query. This needs a batch API in `ipir-sp` and is the cleanest fix because it also
   serves the fixed envelope of §5.
2. Widen ACTION rows to 32 records (~25 KiB rows): four times fewer rows, roughly four
   times the response (~40 KiB).
3. A two-dimensional first dimension (DoublePIR-style).

---

## 5. Query schedule

The memo rule "exactly one real-or-dummy query per completed scan batch" cannot compose
with a recursive traversal whose request count depends on what the wallet owns. Replace it
with a fixed per-pass envelope, versioned as protocol:

```text
per DAG-sync pass, against one generation:
  K_nf    nullifier query pairs   (one COLD + one WARM each)
  K_act   ACTION row queries
  K_wit   witness query pairs     (one tier A + one tier B each)
```

Queues drain oldest-first. Unused slots are filled with dummy queries at a uniformly random
row from the OS CSPRNG. Overflow waits for the next pass. Recursion depth is bounded by the
number of passes, never by an in-pass loop. Real and dummy queries share endpoint, size,
serialisation, timeout and retry policy. The constants (for example 8 / 4 / 4) are the same
for every wallet and change only with a protocol version, for the reason given in the memo
document §9.2: a wallet-local knob becomes a fingerprint.

Adjacent rows for one spent transaction (`action_count > 8`) consume adjacent `K_act` slots
in the same pass when available and otherwise defer, so a large transaction changes nothing
observable.

---

## 6. Deployment

Per environment (staging on `test`, production on `main`), one region to start.

```text
                    ┌───────────── public edge: Caddy TLS, optional onion service ─────────────┐
  wallet ──────────►│   pir.<domain>  →  coordinator-01 / coordinator-02  (no rows, stateless)  │
                    └──────────────────────────────────┬───────────────────────────────────────┘
                                                       │  private VPC, MPQ1 / MPR1 / MPH1 framing
              ┌────────────────────────────┬───────────┴──────────────────┬────────────────────────┐
         action-pool                  witness-pool                    nf-pool
         action-worker-01..NN         witness-worker-01..02           nf-worker-01..MM
         ACTION shards                WITNESS tier A + B shards       NF-COLD bucket ranges
                                                                      NF-WARM replicated on every nf worker

         ownership within a pool = f(shard_id) over that pool's ordered worker list; never rebalanced

  ingest-01  (Zakura archive node, 1 TiB volume)
     ├─► journals: actions.bin / commitments.bin / nullifiers.bin + manifest.json
     ├─► generation artifacts (sealed shard databases, CRS, manifest, sha256) ──► DigitalOcean Spaces
     └─► frontier deltas to workers; workers restore sealed shards from Spaces on (re)start
```

### 6.1 Roles

**ingest.** The only consumer of the archive node. It produces three append-only journals
(ACTION record, cmx leaf, nullifier with `SpendMeta`) with the journal discipline already in
`memo/memo-pir/src/store.rs` (append, `sync_all`, then manifest via tmp + fsync + rename),
cross-checked per block against `trees.ironwood.size`. It runs on its own host; in the POC
it shares the coordinator's. It publishes per-generation artifacts to
`s3://<bucket>/pir/<network>/<generation>/…`, manifest written last with
`Cache-Control: no-cache`, copying the publisher pattern from
`vote-nullifier-pir/.github/workflows/publish-snapshot.yml`.

**coordinator.** Holds no rows. Parses the global query, slices it per shard, sums the
partials, packs once. Two instances behind the edge for availability; both read the
generation manifest and hold only packing state. Any worker failure surfaces as one generic
503. Public routes:

```text
GET  /v1/generation
GET  /v1/{action,witness,nf-cold,nf-warm}/params
GET  /v1/{action,witness,nf-cold,nf-warm}/public-params
GET  /v1/witness/cap
GET  /v1/witness/frontier?from=<height>&to=<height>
POST /v1/{action,witness,nf-cold,nf-warm}/query
```

**workers, one pool per database.** Private port, firewalled by tag to the coordinators.
The pools are separate because the databases have different resource profiles, not
different request rates; the envelope in §5 fixes the ratio of queries across databases.

| Pool | Why it is its own pool |
| --- | --- |
| action-pool | Every query scans every ACTION shard, so per-query cost is proportional to total ACTION bytes (36.9 GiB at 50 M positions). Memory-bandwidth bound, and the only pool that grows continuously. Size it from load tests, not from storage. |
| witness-pool | Small (1.5 GiB at 50 M), sealed shards never rebuild, and it sits on the spendability path where latency matters. Two hosts for availability. Isolation keeps ACTION load and nullifier rebuild bursts away from witness latency. |
| nf-pool | NF-COLD re-preprocesses all of its shards at the daily checkpoint, a CPU and memory-bandwidth burst that would degrade ACTION queries if co-located. NF-WARM is replicated on every nf worker and rebuilt each generation. |

Pools are logical. Each is an ordered worker URL list per database and ownership is
`f(shard_id)` over that list. At today's scale the lists may name the same physical hosts;
splitting later is a configuration change plus artifact restore from Spaces, with no shard
renumbering as long as each list keeps its order. The client never sees a pool, so pool
topology is not a privacy surface.

**generation swap.** All frontier shards prepared and hints summed in every pool before a
single atomic swap across all four databases. Two generations retained.

### 6.2 Operations tooling

Generalise the `vote-nullifier-pir` workflows, which are hard-coded to one service, one
port, one path and a fixed four-host topology (`docs/runbooks/ci-setup.md` there):

- `release.yml`: artifact-only; linux-amd64 with `+avx512f`, plus arm64; mirrored to Spaces.
- `deploy.yml` and `restart.yml`: parameterised by role (`coordinator | worker | ingest`) and
  pool. Rolling order is workers, then coordinators. Readiness gates on `/ready` and on a
  `served_generation >= expected_generation` metric. Adding a worker appends to a pool's
  list. **Reordering a list is an operator error** (`vizor_tx_enhancement.md` §7.2); the
  workflow diffs the list against the published manifest and refuses a reorder.
- `publish-generation.yml`: manual for staging and rollback; automatic from ingest in
  steady state.
- Observability: the `pir-apm` sidecar pattern and a Sentry generation-staleness watchdog.
  Metrics stay aggregate and never carry anything derived from a query. The memo POC
  already ships this: `memo-pir-server` exposes `/metrics` (prefix `memo_`, with
  `memo_snapshot_generation` as the served-generation gauge) and `/ready`, and
  `deploy/pir-apm` runs on the coordinator with its dashboard at `/apm/`.
- GitHub Environments `staging` and `production`. DNS `pir.<domain>` and
  `stage.pir.<domain>`. Terraform under `infra/digitalocean/<env>` with isolated state,
  replacing the `memo-poc` root.

### 6.3 Sizing

Unpadded record bytes across the fleet. Power-of-two row padding and iPIR artifacts
roughly double each figure. Derivations: ACTION = positions × 792; WITNESS = positions × 32;
NF-COLD = nullifiers × 41 ÷ 0.55 with nullifiers ≈ positions as the upper bound.

| Positions | ACTION | WITNESS | NF-COLD | action-pool (64 GB hosts) | witness-pool | nf-pool |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 136 K (today) | 103 MiB | 4.2 MiB | 9.7 MiB | 2, sharing hosts with the other pools | 2 | 2 |
| 1 M | 755 MiB | 31 MiB | 71 MiB | 2 | 2 | 2 |
| 10 M | 7.4 GiB | 305 MiB | 710 MiB | 4–6 | 2 | 2 |
| 50 M | 36.9 GiB | 1.5 GiB | 3.5 GiB | 16–24 | 2 | 2–4 |

Per-query server work stays linear in the size of each database across its pool, as the
memo document §7.2 already notes. The table is capacity, not throughput. Choose worker
counts from a load test at the one-million-position point.

---

## 7. What is still missing

- **The tail scan is mandatory.** Nothing serves `[anchor + 1, tip]` privately. Mempool
  status and `GetStatus(txid)` for wallet-originated transactions remain a separate leak.
- **Fees** are not recoverable from actions alone; they need the value balance and any
  transparent components. History shows amounts and txids, not fees, until a private
  full-transaction retrieval service exists.
- **Mixed pools** (Sapling, pre-Ironwood Orchard, transparent) stay on the legacy path. The
  `MemoPirOnly` classification proof from the memo document §9.5 becomes a
  `DagSyncEligible` proof with the same fail-closed rule.
- **The Shielded Vote `nf-server`** could become a fifth database on the same fleet, but its
  dataset is pinned to a voting round's `snapshot_height`, not tracked by generation. Keep
  it separate until that is reconciled.
- **Research items before wallet traffic:** a batch query API in `ipir-sp` (§4); the
  end-to-end sharded-versus-monolithic transcript test the memo document §11 lists as open;
  an independent parameter review at the 6,336-byte row shape and at each supported
  capacity.

---

## 8. Wallet-side contract

What a wallet integration must implement, in one place so the Vizor repository can point at
it:

1. Fetch `GET /v1/generation`; verify `network`, `anchor_hash` against the locally scanned
   chain, and `ironwood_tree_size`. Send nothing before the anchor has been scanned.
2. Pin that generation for the whole pass. If any response carries a different generation,
   discard it and end the pass; the next pass starts from the new generation.
3. Issue exactly `K_nf` nullifier pairs, `K_act` ACTION queries and `K_wit` witness pairs,
   dummies included. Never a result-dependent extra request, never a fallback to
   `GetTransaction(txid)` or a block download for the covered class.
4. Authenticate every decrypted action with full Ironwood note decryption over the
   580-byte ciphertext; compare recovered notes against the leaf returned by witness
   tier B; persist only then. Never create or alter a note from an unauthenticated response.
5. Treat a nullifier answer as an assertion by the server (§3.3), and scan the tail
   `[anchor + 1, tip]` locally before declaring a note spendable.
6. Cache witnesses for notes in sealed shards permanently; update them from the cap and the
   frontier broadcast; re-query only notes in the frontier shard.
7. Log failures as one aggregate category with no txid, position, row, slot or plaintext.

---

## 9. References

- [`vizor_tx_enhancement.md`](vizor_tx_enhancement.md) — memo PIR design, sharded
  coordinator, deployment POC
- [`pir_wallet_integration.md`](pir_wallet_integration.md) — current nullifier and witness
  wallet integration
- `../nullifier/README.md`, `../witness/README.md` — current database layouts and rebuild
  costs
- `../plans/future/frontier_witness_update_design_1bab3c5a.plan.md` — rightmost-path
  broadcast adopted in §3.2
- `../plans/wip/decryption_pir_implementation_4a3d65f2.plan.md` — subsumed by §3.1
- `vote-nullifier-pir/docs/runbooks/ci-setup.md`, `server-setup.md` — fleet tooling
  generalised in §6.2
