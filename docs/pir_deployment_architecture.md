# Unified PIR deployment for DAG-sync

Status: design, 2026-09-02; scope revised 2026-09-03. Sections 2 through 5 are implemented
(server, `wallet-libraries`, Vizor). Section 6 was rewritten to the production shape that
actually ships. It builds on the memo PIR described in
[`vizor_tx_enhancement.md`](vizor_tx_enhancement.md) and describes how the nullifier,
witness and transaction-enhancement services became one coordinator that a DAG-sync wallet
can use.

## Scope status

**Shipped scope: memo retrieval only.** The production fleet serves the ACTION table and
nothing else (`--tables action`). That removes the one leak this program set out to remove
first: the wallet's exact-txid `GetTransaction` request after compact scanning. Everything
in this document that exists for DAG-sync is implemented but is not a supported product
path:

| Piece | State | Scope |
| --- | --- | --- |
| ACTION table, 824-byte record (§3.1) | deployed, production | shipped |
| One generation manifest, pinned fetches, retained generations (§3.4) | deployed | shipped |
| WITNESS and WITNESS-ROOTS tables, cap, frontier (§3.2) | implemented, not served | behind `--tables` |
| NF-COLD and NF-WARM tables (§3.3) | implemented, not served | behind `--tables` |
| Query envelope 8 / 4 / 4 (§5) and the Vizor DAG-sync pass | implemented | stands down when tables are absent |
| Three worker pools, two coordinators, Spaces artifacts (§6.4) | not built | growth path, not planned |

Why DAG-sync is out of scope: it is a UX feature (spendability, change and history before
the scan reaches the tip), not a privacy fix. Its privacy contribution, removing the block
download for change discovery, is already covered by the ACTION record. Its cost is a
fixed 28-query envelope per pass against one query per batch, four more tables, and the
fleet in §6.4. It stays behind the flag until restore-from-seed UX becomes a priority.

Next milestone: the transparent-address table (§7.1). It is the second leak on the list and
it reuses the cold/warm hashtable framework that §3.3 built.

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
nf[32] ‖ ephemeralKey[32] ‖ encCiphertext[580] ‖ cmx[32] ‖ cv_net[32] ‖ outCiphertext[80] ‖ txid[32] ‖ height[4]
= 824 bytes            8 per row = 6,592-byte row            row = p >> 3, slot = p & 7
```

(Record v3. The design below was written at 792 bytes without `cmx`; `cmx` was added at
implementation time so a note discovered from an action row can be authenticated without a
witness query. Row and instance arithmetic below is updated to 824.)

Why each addition is required, named by the DAG-sync step that fails without it:

| Field | Bytes | Needed by |
| --- | ---: | --- |
| `nf` (the action's spent nullifier, which is `rho` of the new note) | 32 | Step 2. Ironwood note decryption takes `rho` from the action. The 612-byte memo record cannot decrypt an unknown note at all; it can only complete a memo for a note that compact scanning already found. |
| `cv_net`, `outCiphertext` | 112 | Outgoing recovery under the OVK after a seed restore. Without them sent history is unrecoverable except by requesting transactions by txid. The memo document preferred a second column; one record is better here because DAG-sync reads the row anyway, and a second column is a second query in the fixed schedule of §5. |
| `txid`, `height` | 36 | Transaction history and confirmation depth without any lightwalletd call. Today change discovery downloads the compact block at `spend_height` from lightwalletd (`nullifier/README.md`, "Change Note Discovery"), a block-level leak that must go. |

`cmx` was first omitted (recomputed from the decrypted note, with witness tier B returning
the leaf) and then included in v3 so that authentication does not depend on a witness
query.

The `DecryptionLeaf { nf, ephemeral_key, ciphertext[52] }` record from
`plans/done/8_decryption_pir_implementation_4a3d65f2.plan.md` is subsumed by this record
and that plan is closed against this document.

Geometry, confirmed by `params_for_simplepir` (test `action_rows_use_two_ipir_instances`):
with `d = 2048` and `p = 2^14` one iPIR instance carries 28,672 plaintext bits, so the
6,592-byte row (52,736 bits) still fits `instances = 2`. `db_cols`, the request, and the
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

A pass pins one generation for all of its queries. The coordinator retains eight generations so an
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

What ships is the memo fleet as built, renamed from proof of concept to production. One
environment, one region, one table.

```text
                    ┌──────────── public edge: Caddy TLS on pir.<domain> ────────────┐
  wallet ──────────►│  coordinator-01: Zakura archive (1 TiB), ingest, coordinator,   │
                    │                  pir-apm sidecar                                 │
                    └──────────────────────────┬───────────────────────────────────────┘
                                               │  private VPC, worker port 8091 by tag
                              worker-01 ───────┴─────── worker-02   (append-only list)
                              ACTION shards, ownership = f(shard_id) over the list
```

### 6.1 Roles

**coordinator-01.** Runs `zakurad` (archive), the ingest loop, and `memo-pir-server
--tables action`. Ingest writes every table's journal (ACTION, commitments, sub-shard
roots, nullifiers) with the append-then-manifest discipline in `memo/memo-pir/src/store.rs`,
so widening the served set later is a flag change, not a re-ingest. Publication is gated at
`tip − CONFIRMATION_DEPTH`; the coordinator retains eight generations and pins parameter
fetches to a generation. Holds no rows. Any worker failure surfaces as one generic 503.
Public routes:

```text
GET  /v1/generation
GET  /v1/action/params
GET  /v1/action/public-params
POST /v1/action/query
GET  /v1/health
```

The witness and nullifier routes exist in the binary and return 503 or an absent-table
error when their tables are not served.

**workers.** Private port, firewalled by tag to the coordinator. One ordered list in
`/etc/memo-pir/workers.json`; ownership is `f(shard_id)` over that list and the list is
append-only. Sealed shards are immutable and hash-verified on load.

### 6.2 Operations

- `.github/workflows/deploy-pir-fleet.yml` deploys the commit that passed `CI` on `main`
  to the `production` GitHub Environment, workers first, then the coordinator, with
  checksum-verified uploads, health gates, and rollback. Runbook:
  [`memo-pir-deploy.md`](memo-pir-deploy.md).
- The deploy script refuses a worker inventory that is not a valid append to the previous
  one only by convention today; the manifest carries each shard's owning worker, so a
  reorder shows up as digest mismatches at activation.
- Observability is the `pir-apm` sidecar on the coordinator (`/apm/`), aggregate metrics
  only. No Sentry watchdog.
- Terraform under `infra/digitalocean/production`, state untracked, remote state on Spaces
  as the documented next step. No staging environment.

### 6.3 Sizing

ACTION is the only served table. Unpadded record bytes; power-of-two row padding and iPIR
artifacts roughly double the figure.

| Positions | ACTION bytes | Hosts |
| ---: | ---: | --- |
| 136 K (today) | 107 MiB | 1 coordinator + 2 workers, as deployed |
| 1 M | 786 MiB | same; load test before adding a worker |
| 10 M | 7.7 GiB | add workers to the list; still one pool |

Per-query server work is linear in the size of the table across the worker list. Choose
worker counts from a load test, not from storage.

The trigger that matters is not storage but the request: the SimplePIR first-dimension
query grows linearly in `logical_rows`, and at roughly three Ironwood positions per block
today the pool reaches one million positions within about a year. At that point the request
is about 700 KiB. Before then, one of: amortise the packing keys across a batch of queries
(batch API in `ipir-sp`), widen ACTION rows to 32 records, or a two-dimensional first
dimension. Any of these is a client and server change; the row widening also rebuilds every
sealed shard, which is why it should be decided before the pool is large.

### 6.4 Growth path, not planned

The original §6 designed for DAG-sync at 50 M positions. Kept here in condensed form so the
reasoning survives; none of it is scheduled.

- **One worker pool per table** (`action-pool`, `witness-pool`, `nf-pool`), because the
  tables have different resource profiles: ACTION is memory-bandwidth bound and grows
  continuously; WITNESS is small and latency-sensitive; NF-COLD re-preprocesses everything
  at its daily checkpoint. Pools are logical (an ordered worker list per table), so splitting
  is a configuration change plus artifact restore, with no shard renumbering.
- **Two stateless coordinators** behind the edge for availability, both reading the
  generation manifest.
- **A separate ingest host** publishing per-generation sealed-shard artifacts and the
  manifest to DigitalOcean Spaces, with workers restoring sealed shards from Spaces on
  restart (the `vote-nullifier-pir` publish-snapshot pattern).
- **Role- and pool-parameterised** `deploy`, `restart` and `publish-generation` workflows
  with a readiness gate on `served_generation >= expected_generation` and a refusal to
  reorder any pool's list.
- **Staging** on `test`, `stage.pir.<domain>`, isolated Terraform state.
- At 50 M positions: ACTION ≈ 38 GiB across 16–24 hosts, WITNESS ≈ 1.5 GiB on 2,
  NF-COLD ≈ 3.5 GiB on 2–4.

---

## 7. What is still missing

### 7.1 Transparent addresses: the next milestone

Vizor still sends its transparent addresses to lightwalletd in two places: the
address-scoped txid stream used for gap-limit discovery and history
(`TransactionsInvolvingAddress` in `rust/src/wallet/sync_engine/enhance.rs`) and the UTXO
stream used for balance (`download_transparent_outputs` in `sync_engine/mod.rs`). That
reveals every t-address the wallet owns, links them to each other, and links them to the
shielded session. Tor hides the network address, not the addresses in the request.

This is a hash-keyed problem, not a position-indexed one, so the ACTION framework does not
apply and the NF-COLD / NF-WARM framework of §3.3 does:

- **Key**: hash of the output script (`hash(script_pubkey)`), bucketed like `hash(nf)`.
- **Record**: a capped list of unspent outputs for that script (`txid ‖ index ‖ value ‖
  height`, 48 bytes each) plus a `used` flag and a total count, so one lookup answers both
  "balance" and "has this address ever been used" for gap-limit discovery.
- **Cold / warm**: the UTXO set churns every block, exactly like the nullifier set. Daily
  cold checkpoint over the full UTXO set, warm delta since the checkpoint that also carries
  spends of cold entries, and the wallet always issues one cold and one warm query per
  address.
- **Overflow**: addresses with thousands of UTXOs (exchanges, mining pools) exceed any
  fixed cap. Cap the list, set a `truncated` flag, and have the wallet fail closed to
  "balance unknown until scanned" for such an address rather than fall back to the
  lightwalletd stream, which would be a result-dependent request.
- **Schedule**: a fixed number of address pairs per pass, dummies included, as in §5. The
  gap-limit walk must not become a variable number of requests.
- **What it does not hide**: that the wallet is a PIR client, and whatever the wallet still
  sends to lightwalletd for broadcasting transparent spends.

Size at today's mainnet UTXO set is a few hundred megabytes at the 41-byte-entry density
of §3.3; it fits the existing coordinator and worker list as a second served table
(`--tables action,t-utxo-cold,t-utxo-warm`). The ingest already reads full blocks from
Zakura, so the journal is an addition, not a new source.

### 7.2 Open items

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
- **Research items before DAG-sync traffic:** a batch query API in `ipir-sp` (§4, also the
  ACTION request-size trigger in §6.3); an independent parameter review at the 6,592-byte
  row shape and at each supported capacity. The sharded-versus-monolithic transcript test
  from the memo document §11 exists (`memo/memo-pir/tests/sharded_vs_monolithic.rs`).

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
- [`memo-pir-deploy.md`](memo-pir-deploy.md) — the production deploy workflow and runbook
- `../plans/done/8_decryption_pir_implementation_4a3d65f2.plan.md` — subsumed by §3.1
- `vote-nullifier-pir/docs/runbooks/ci-setup.md`, `server-setup.md` — fleet tooling the
  growth path in §6.4 would generalise
