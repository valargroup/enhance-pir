# Vizor Ironwood Memo PIR

Status: design and deployed proof of concept, 2026-09-02. The POC is not wired into
Vizor. This document describes the first transaction-enhancement privacy milestone —
private recovery of incoming and account-internal **Ironwood memos**. It is not an
implementation of all wallet transaction enhancement.

It replaces both the earlier design draft and the separate POC execution plan.
[`roman_notes.md`](roman_notes.md) is retained as superseded historical context.

---

## 1. Summary

Vizor scans compact blocks locally, then asks lightwalletd for the exact transaction ID
of each wallet-relevant transaction. That enhancement request tells the server which
transaction is yours.

This milestone removes that request for one class of transaction. Compact scanning
already assigns every received Ironwood note its global note-commitment-tree position,
so that position is used directly as a PIR index. There is no txid directory, no keyword
PIR, and no cuckoo hashing.

Each record holds exactly what a compact block omits:

```text
nf[32] || ephemeralKey[32] || encCiphertext[580] || cv_net[32] || outCiphertext[80] || txid[32] || height[4]
                                                                              = 792 bytes
```

Eight consecutive action records form one 6,336-byte PIR row. Given an Ironwood position
`p`:

```text
row  = p >> 3
slot = p & 7
```

The server exposes **one global logical database** for the latest finalized snapshot.
Clients never select a physical shard. The wallet issues exactly one real-or-dummy memo
query after each completed eligible compact-scan batch, regardless of how many notes
matched — so the server learns neither the matched txid, nor the position, nor whether
the batch produced a target at all.

What was built: a standalone Rust crate (`memo/memo-pir`) with a coordinator, private
row-shard workers, and a reference client; a distributed iPIR+SP evaluator; and a
three-host DigitalOcean deployment serving the full finalized Ironwood pool from
activation. As of the latest check the service reports 136,425 positions across two
sealed shards and one frontier shard.

Out of scope for version 1: raw transaction reconstruction, outgoing recipient data,
fees, transparent history, other shielded pools, and unmined transaction status.

---

## 2. The leak, and why a position index closes it

Compact Ironwood actions carry enough ciphertext to *discover* a note (the first 52
bytes) but not the encrypted memo or its authentication tag. So the pinned
`zakura-client-backend` flow queues an `Enhancement(txid)` and Vizor calls lightwalletd
`GetTransaction(txid)`:

```text
compact-block download
  -> local trial decryption and spend detection
  -> wallet-relevant txid queued for enhancement
  -> GetTransaction(txid)
  -> full transaction decrypted and stored locally
```

Compact scanning alone identifies nothing to the server. The exact-txid request does. A
lightwalletd operator can link the connection to that transaction and cluster repeated
requests as one wallet. Tor hides the network address; it does not hide txids, and it
does not unlink requests made in one application session.

An earlier design proposed a txid directory, two cuckoo lookups, action-group pages, and
a second memo PIR on top. All of that existed to answer "where is this txid's data?" —
a question the wallet does not need to ask. Compact scanning already produced the global
commitment-tree position, which is:

- **dense and append-only** — every Ironwood action occupies exactly one position;
- **independent of transaction size and txid** — a transaction with many actions simply
  occupies many consecutive positions; and
- **collision-free** — no posting lists, no cuckoo candidates, no oversized-transaction
  fallback.

The trade is deliberate: this index only serves notes the wallet already found. It is not
a universal transaction index. Spend-only transactions, other pools, and transparent-only
transactions have no useful Ironwood output position.

---

## 3. Threat model and claims

**Adversary.** One party controls the memo PIR service and observes every request on a
client connection. It may record timing, size, snapshot identifier and connection
metadata; correlate all requests on a connection; return stale, malformed or adversarial
responses; selectively fail requests; and control every worker behind the public
endpoint. It does not hold the wallet's viewing keys and does not compromise the device.

Version 1 uses a single computational-PIR operator. **There is no non-collusion
assumption between workers** — they are all the operator's. TLS is still required for
transport authentication, but query privacy does not depend on TLS or Tor hiding the
selected index.

**Privacy.** Assuming the query-privacy property of the pinned iPIR+SP version and
parameter set, the server's view of a valid query does not reveal its logical row. The
application layer adds that requests contain no txid, position, row, slot or shard
identifier in plaintext; that every completed eligible scan batch produces exactly one
fixed-shape query; and that real and dummy queries share endpoint, snapshot, connection
policy, serialization, timeout and retry policy.

**Integrity.** PIR privacy authenticates nothing. The client runs the protocol's existing
Ironwood authenticated note decryption over the whole 580-byte `encCiphertext` including
its 16-byte tag, then compares the decrypted note against the note already stored by
compact scanning. Under the authenticity of Ironwood note encryption and the commitment
binding checked during scanning, a malicious server cannot make the wallet accept a
forged memo for a different stored note.

**Not claimed.** This hides neither that the client uses the service, nor when a
compact-scan batch completed, nor which public snapshot generation was current, nor that
all requests belong to one connection. The server can still censor, delay, replay an
older snapshot, or deny service. A fixed one-query budget does not promise immediate
recovery when many notes arrive at once. And the design removes one concrete request
class — it is not a claim that all Vizor network activity becomes private.

No new cryptographic primitive, encryption mode, nonce, key derivation, or handwritten
authentication check is introduced anywhere in this design.

---

## 4. Database layout

### 4.1 Record

One logical record is 792 bytes (`ActionRecord` in `memo/memo-pir/src/types.rs`):

| Offset | Size | Field |
| ---: | ---: | --- |
| 0 | 32 | The action's spent nullifier `nf` (this is `rho` of the output note) |
| 32 | 32 | Canonical Ironwood `ephemeralKey` encoding |
| 64 | 580 | Complete Ironwood `encCiphertext` |
| 644 | 32 | `cv_net` |
| 676 | 80 | `outCiphertext` |
| 756 | 32 | `txid`, internal (little-endian) byte order |
| 788 | 4 | Block height, little-endian `u32` |

The first three fields are what memo completion needs. The rest exist for DAG-sync
([`pir_deployment_architecture.md`](pir_deployment_architecture.md) §3.1): `nf` lets a
wallet trial-decrypt a note it has not seen, `cv_net` and `outCiphertext` allow outgoing
recovery under the OVK, and `txid` plus height give history without any lightwalletd call.
`cm_x` is omitted because the wallet recomputes it from the decrypted note. The record was
widened before any wallet traffic because widening later invalidates every sealed shard.

The ciphertext is the complete on-chain value. The compact block's first 52 bytes are not
enough: the remaining 528 hold the encrypted 512-byte memo region and the 16-byte
authentication tag. Fetching only the 512 memo bytes would drop authentication and is
forbidden.

The record deliberately repeats the compact prefix and the ephemeral key. That makes a
pending memo recoverable after a restart from the note and position the wallet already
persists, with no separate cache of ephemeral compact-block material.

It carries no `cmx`. For memo completion the client already has the note, its `rho`, its
position and its key scope, and ignores the other fields.

### 4.2 Row and logical capacity

```text
RECORD_BYTES     = 792
RECORDS_PER_ROW  = 8
ROW_BYTES        = 6,336
record_offset    = slot * 792
```

Eight records keep the arithmetic exact and put the row comfortably above the SimplePIR
minimum row size. The client validates the exact response length, decodes the padded
plaintext, and treats only the first 6,336 bytes as the row.

Capacity is a public power of two (`types.rs:111-113`):

```text
used_rows    = ceil(ironwood_tree_size / 8)
logical_rows = max(used_rows, 8_192).next_power_of_two()
```

Rows past `used_rows`, and unused slots in the last used row, hold one canonical all-zero
792-byte padding record. Padding is never eligible for decryption or persistence. The
power of two reduces rebuild frequency and keeps shard boundaries predictable; it is not
itself a privacy mechanism.

At the live snapshot (`ironwood_tree_size = 136,425`):

```text
used_rows          = 17,054
logical_rows       = 32,768        position capacity 262,144
database bytes     = 32,768 * 6,336 = 207,618,048 = 198 MiB
```

At 50 million positions, unpadded record data alone is ~36.9 GiB, before PIR
preprocessing.

### 4.3 Physical shards

| Constant | Value |
| --- | ---: |
| Rows per shard | 8,192 |
| Positions per shard | 65,536 |
| Raw padded bytes per shard | 51,904,512 (49.5 MiB) |
| Shards per worker (POC) | 2 |

Defined in `memo/memo-pir/src/types.rs:8-18`. Physical layout is invisible to the client;
it exists so ingestion, storage, preprocessing and evaluation can be parallelised.

### 4.4 On disk — coordinator journal

Two files in the coordinator's data directory (`memo/memo-pir/src/store.rs:51-52`):

- `records.bin` — a flat, unframed concatenation of 792-byte records starting at
  `base_position`. Position to offset is `(position - base_position) * 792`.
- `manifest.json` — `{version, base_position, tree_size, blocks: [{height, hash,
  first_position, action_count}]}`.

Commit order is append → `sync_all()` on `records.bin` → only then bump `tree_size`,
push the block entry, and persist the manifest as tmp + fsync + rename + directory fsync
(`store.rs:134-151`, `store.rs:208-218`). On restart a `records.bin` **shorter** than the
committed length is a hard error; a longer one has its uncommitted tail truncated
(`store.rs:73-89`). Shard reads are zero-padded to full geometry, so the frontier shard
is deterministically filled (`store.rs:175-180`).

`rows_digest` — hex SHA-256 over the full padded 51,904,512-byte shard buffer
(`store.rs:200-202`) — is the single identity threaded through prepare, activate, and the
published metadata.

### 4.5 On disk — worker artifacts

One directory per shard, `shard-{id:08}`, holding three files
(`memo/memo-pir/src/ipir.rs:210-231`):

| File | Contents |
| --- | --- |
| `database.u16le` | The transposed iPIR server database, `db_rows * db_cols` u16 LE |
| `partial-crs.bin` | That shard's partial CRS blocks in `MPH1` framing |
| `metadata.json` | Version, `d`, `q`, `p`, `db_rows`, `db_cols`, `shard_id`, `query_row_start`, `rows_sha256`, `database_sha256`, `crs_sha256` |

All three are written tmp + `sync_all` + rename, with a directory fsync
(`ipir.rs:237-244`). On load every one of those fields is checked, and both the database
and the CRS blob are re-hashed (`ipir.rs:147-206`). A sealed shard therefore survives a
worker restart without being rebuilt, and a corrupted or mismatched artifact fails
loudly rather than serving wrong bytes.

---

## 5. Distributed iPIR+SP: what had to change

The POC is pinned to iPIR-SP revision `e875404cef33661906ab60af236dfb327e6b28b1`
(`memo/memo-pir/Cargo.toml:27-28`) — the commit *"Add row-sharded IPIR combination
primitives"*. The rest of this workspace still runs revision `2bc1075`. That revision
is `+176/-19`, entirely in `ipir-sp/src/server.rs`, and adds three things.

**1. A free-standing query parser.**

```rust
pub fn deserialize_first_dim_query(
    rlwe: &RlweParams, ypir: &YpirSchemeParams, query: &[u8],
) -> Result<Vec<u64>, InspiringError>
```

This was a private `YServer` method. Lifting it out lets a coordinator parse one client
query against the **global** geometry without owning a database — which is the whole
point, since the coordinator holds no rows.

**2. Online partial combination.**

```rust
pub fn add_intermediate_assign_mod(
    accumulator: &mut [u64], contribution: &[u64], modulus: u64,
) -> Result<(), InspiringError>
```

Sums shard first-dimension outputs in `u128` before reducing, and rejects a length
mismatch or any input `>= modulus`. A worker cannot smuggle a non-canonical residue past
it.

**3. Offline partial combination.**

```rust
pub fn add_crs_blocks_assign_mod(
    accumulator: &mut [CrsBlock], contribution: &[CrsBlock], params: &RlweParams,
) -> Result<(), InspiringError>
```

The same operation for offline CRS blocks, shape-strict: block counts must match and each
block must be exactly `d x d`.

### 5.1 Why row sharding is sound here

Two invariants carry the whole construction.

**Shard outputs have the same width as the global one.** The column count
`db_cols = instances * 2048` is derived from `ITEM_SIZE_BITS` — the 6,336-byte row — and
not from the row count. One instance carries `d * log2(p) = 28,672` plaintext bits, so a
50,688-bit row still needs `instances = 2`, `db_cols = 4096`, exactly as the original
4,896-byte row did (test `action_rows_use_two_ipir_instances`)
(`memo/memo-pir/src/ipir.rs:304-310`). A shard of 8,192 rows and the global database of
32,768 rows therefore both produce a 4,096-wide intermediate, and the intermediates are
simply addable modulo `q`.

**The public setup is prefix-stable.** Setup polynomials are generated deterministically
from the protocol seed whose `u64` value is the first eight little-endian bytes of
`SHA-256("zcash/ironwood-memo-pir/setup-seed/v1")`. The coordinator publishes that value
as `setup_seed`, and clients reject snapshots advertising any other value. Growing global
capacity only *extends* the sequence — the upstream test
`public_setup_is_prefix_stable_when_global_capacity_grows` asserts exactly this at the
memo shape. That lets a shard take the slice `global_setup[first_poly .. first_poly + 4]`
with `first_poly = query_row_start / d` (`ipir.rs:113-118`), and it is why a
power-of-two capacity doubling leaves every sealed shard's CRS untouched.

The seed is part of the preprocessing protocol, not merely metadata. Changing it bumps
the persisted artifact version and requires a one-time rebuild of every shard. Ordinary
snapshot growth still rebuilds only the mutable tail shard; sealed older shards remain
stable.

### 5.2 Equivalence evidence

Upstream, at the pinned revision:

- `row_shard_intermediates_sum_to_monolithic_result` — a 16x8 monolithic server versus two
  8-row shards over the same database; the summed partials equal the monolithic product
  exactly.
- `row_shard_crs_contributions_sum_to_monolithic_crs` — each shard precomputes over its
  *slice* of the setup polynomials; the summed partials equal the monolithic CRS blocks.
- `distributed_combiners_reject_malformed_contributions` — width mismatch, degenerate
  `CrsBlock`, and a value equal to `q` are all rejected.
- `public_setup_is_prefix_stable_when_global_capacity_grows` — at the real memo geometry,
  8,192 rows versus 32,768.

**There is no end-to-end memo-level sharded-versus-monolithic test yet.** The algebra is
covered in the library; equivalence of the full coordinator transcript against a
monolithic evaluation is an open item, not a completed gate. See §11.

---

## 6. How the coordinator aggregates

```text
                     one opaque global query (no shard selector)
   client ─────────────────────────────────────────────► coordinator
                                                            │
                            packing keys stay here ─────────┤
                                                            │  slice [row_start .. +8192]
                                    ┌───────────────────────┴───────────────────────┐
                                    ▼                                               ▼
                              worker-1                                        worker-2
                          shards 0..1                                     shards 2..3
                        b_s = D_sᵀ q_s                                   b_s = D_sᵀ q_s
                        fold locally                                     fold locally
                                    └───────────────────────┬───────────────────────┘
                                                            │  Σ b_s mod q
                                                            │  pack ONCE, modulus-switch
   client ◄─────────────────────────────────────────────────┘
                     generation ‖ epoch ‖ packed response bodies
```

### 6.1 Online path

`coordinator.rs:435-518`:

1. Check the leading 8-byte `generation` against the live snapshot, and check the body
   against one exact expected length —
   `8 + serialized_packing_keys_len(rlwe) + ceil(db_rows * query_bits / 8)`. Any other
   length is rejected before allocation.
2. Deserialize the packing keys. **Workers never see them.**
3. Deserialize one global first-dimension query — a `db_rows`-long coefficient vector.
4. For each published shard, slice `[global_row_start .. global_row_start + 8_192]`, and
   group the slices by the owning worker. A gap in shard coverage is an error, not a
   partial answer.
5. Fan out with a `JoinSet` and fold every reply into a 4,096-wide accumulator with
   `add_intermediate_assign_mod(&mut combined, &partial, q)`.
6. Pack **exactly once**, at the coordinator, with
   `pack_intermediate_blocks(&combined, &packing_keys, &top_key_images, &preprocessed)`,
   then modulus-switch the bodies down to `q'_1 = 2^20`.
7. Reply `generation[8] ‖ public_params_epoch[8] ‖ c2 bodies`.

The expensive response packing therefore happens once per query regardless of shard
count, and the workers do only the linear first-dimension product.

Worker side (`worker.rs:198-230`, `ipir.rs:132-145`): the requested shard-ID set must
equal that generation's **complete** active assignment; each slice must be exactly 8,192
coefficients and every coefficient must be a canonical residue `< q`; the worker computes
`b_s = D_sᵀ q_s` per shard via `multiply_query` and folds its own shards locally before
replying.

### 6.2 Offline path

During publication (`coordinator.rs:179-253`), each worker returns its shard's partial
CRS from `perform_offline_precomputation_simplepir` over its slice of the seeded setup.
The coordinator:

1. sums the partials with `add_crs_blocks_assign_mod`;
2. derives `build_pack_preprocessed_blocks` → `TopKeyImages::build` →
   `published_c1_rows`; and
3. computes an 8-byte `public_params_epoch` from the SHA-256 of the published `c1`.

Hints are cached under `worker:shard:query_row_start:digest`, so only the frontier
shard's hint is ever refetched — every sealed shard hits the cache.

The published `parameter_id` hard-codes the pinned revision, e.g.
`ipir-sp-e875404-d2048-p16384-rows32768-cols4096`. Unknown parameter identifiers are
rejected by clients, never negotiated downward.

### 6.3 Framing and hardening

Coordinator-to-worker messages use fixed little-endian binary framing with magics `MPQ1`
(evaluate request), `MPR1` (evaluate response) and `MPH1` (CRS hint), in
`memo/memo-pir/src/wire.rs`. Every decoder validates lengths before allocation, caps
shard counts, and rejects trailing bytes. The coordinator caps worker bodies at 80 MiB
for a hint and 1 MiB for an evaluate reply, and rejects any reply whose generation
differs from the request.

Public endpoints:

```text
GET  /memo/health          phase, anchor height, tree size, worker count
GET  /memo/metadata        MemoSnapshotMetadata (see §7.4)
GET  /memo/params          pinned iPIR scheme parameters
GET  /memo/public-params   published c1 packing material
POST /memo/query           opaque query -> opaque fixed-shape response
```

Private worker endpoints are `/internal/{health, shards/:id, shards/:id/load,
shards/:id/hint, activate, evaluate}` and are not reachable from the internet (§7.1).

**The privacy-relevant properties**: the client submits no shard or worker identifier;
the coordinator alone slices; every published shard participates in every query; and any
worker failure surfaces as one generic 503 with the reason logged only server-side —
never a shard-specific client response.

---

## 7. Deployment, growth, and horizontal scaling

### 7.1 Deployed topology

Terraform root `infra/digitalocean/memo-poc`, isolated state, attached to the existing
`spendability-pir` DigitalOcean project without adopting the older
`spendability-pir-01` host.

| Host | Size | Role |
| --- | --- | --- |
| `spendability-memo-pir-coordinator-01` | `m-8vcpu-64gb-intel` | Zakura archive, ingestion, publication, public query API |
| `spendability-memo-pir-worker-01` | `m-8vcpu-64gb-intel` | Shards 0–1 |
| `spendability-memo-pir-worker-02` | `m-8vcpu-64gb-intel` | Shards 2–3 |

All in `ams3` on a dedicated VPC `10.142.0.0/24`. The coordinator carries a 1 TiB XFS
volume mounted at `/srv/zakura` for the archive and the memo journal.

Firewalls: SSH restricted to an operator CIDR; 80/443 and Zakura P2P 8233 public on the
coordinator; **worker port 8091 restricted by source tag to the coordinator** — there is
no public worker port. Caddy terminates TLS for `memo-pir.167.99.42.60.sslip.io` in front
of `127.0.0.1:8080`.

Units: `zakurad.service` (archive mode, cookie RPC auth on `127.0.0.1:8232`, 50 MiB
response body cap for raw `getblock`), `memo-pir-server.service` (`--mode
distributed-full` with both `--worker-url` values in the order that fixes ownership),
and `memo-pir-worker.service`.

### 7.2 Placement and horizontal growth

Ownership is a pure function of shard id (`types.rs:115-124`):

```text
worker_index_for_shard(shard_id) = shard_id / SHARDS_PER_WORKER   // 2 in the POC
```

with worker order taken from `--worker-url` order. Consequences:

- **Adding a worker extends capacity into the next shard range and never rebalances.**
  Sealed shards stay where they are, and are never rebuilt or re-preprocessed
  (test `adding_workers_does_not_move_sealed_shards`).
- **Reordering the URL list changes ownership and is an operator error.** There is no
  safety net for it; the artifact digests would mismatch and shards would be rebuilt on
  the wrong hosts.
- **Only the frontier shard churns.** A shard seals at 65,536 positions; after that its
  digest never changes, so its cache key hits, its artifact is reloaded and hash-verified
  rather than rebuilt, and its CRS hint is not refetched. Appending records rebuilds
  exactly one shard.
- **Capacity doubling is cheap** because the seeded setup is prefix-stable (§5.1) —
  growing `logical_rows` from 32,768 to 65,536 does not invalidate sealed shard CRS.

The honest limit: sharding buys capacity and parallel wall-clock, not asymptotics. Every
shard must participate in every query, so total server work per query stays `O(N)` in the
size of the pool. Scaled out:

| Pool size | Records | Shards | Workers at 2 shards each |
| ---: | ---: | ---: | ---: |
| 136 K (today) | 103 MiB | 3 | 2 |
| 1 M | 755 MiB | 16 | 8 |
| 50 M | 36.9 GiB | 763 | 382 |

Two shards per worker is a POC constant chosen to exercise multi-worker placement, not a
law — an `m-8vcpu-64gb-intel` host holds far more than 99 MiB of shard data. The table
is the shape of the problem, not a procurement plan. What it does show is that at
two orders of magnitude of growth the linear-work property, not the storage, is what
needs a different construction.

### 7.3 Generations

`generation = anchor_height`. All shards are prepared and all hints summed **before** any
worker is activated; only then does the coordinator swap the live snapshot with a single
`ArcSwapOption::store`. Workers retain the two newest generations, so an in-flight query
finishes against the generation it started on. A response is never served under metadata
from a different generation.

### 7.4 Ingestion and finality

The coordinator ingests full raw blocks from Zakura's authenticated local JSON-RPC
(cookie auth), pinned to revision `1faf150fc3648aae22c55a6b30f8f5a9b9ce934e`. Lightwallet
compact blocks are not sufficient — they omit exactly the ciphertext suffix and tag this
database exists to serve. A production deployment must likewise use a consensus-validating
source.

Per block it fetches the raw block (`getblock [h, 0]`) and the verbose block for
`trees.ironwood.size` (`getblock [h, 2]`), walks transactions in canonical block order
then Ironwood actions in canonical transaction order, and appends
`ephemeralKey ‖ encCiphertext` at each action's global position.

Publication is gated at `tip - 10` confirmations. Before committing, the cumulative
action count is cross-checked against the block's own published Ironwood tree size; any
gap, duplicate, ordering disagreement, malformed field, or tree-size mismatch blocks
publication rather than producing a partial snapshot. The last committed block hash is
re-verified against Zakura at startup and at the top of every poll; a change aborts
ingestion. A finalized tip regressing below the committed height is fatal. Failures set a
generic `Failed` phase and retry after 30 s.

Published metadata binds: schema version, network, pool, anchor height and block hash,
Ironwood tree size, coverage, record and shard geometry, `used_rows`, `logical_rows`,
generation, `parameter_id`, `public_params_epoch` and digest, and a per-shard descriptor
carrying `rows_sha256`, `sealed`, and the owning worker.

Two modes exist. `distributed` requires at least two workers, starts at Ironwood
activation (height 3,428,143), and refuses to publish unless the entire finalized pool is
continuous and queryable. `embedded` runs one in-process worker over the same full pool
and exists for tests and local development only; the earlier windowed development mode,
which advertised a bounded coverage window, was removed because every client rejects
partial coverage.

---

## 8. Query time and bandwidth

Numbers below are labelled **measured**, **derived**, or **target**. They are not
interchangeable.

### 8.1 Derived from the pinned parameters

`d = 2048`, `q = 72,057,594,037,641,217` (56 bits), `p = 2^14`, gadget `ell = 3`,
`instances = 2`, `db_cols = 4096`, `q'_1 = 2^20`. At today's `logical_rows = 32,768`
(`query_bits = 42`):

| Item | Bytes | Frequency |
| --- | ---: | --- |
| Packing keys | 86,016 (84 KiB) | every query |
| First-dimension query | 172,032 (168 KiB) | every query |
| **Request total** | **258,056 (252 KiB)** | every query |
| **Response** (`8 + 8 + 2 x 5,120`) | **10,256 (10 KiB)** | every query |
| `/memo/public-params` (`c1`) | 28,672 (28 KiB) | once per epoch |
| `/memo/metadata` | ~300 B per shard | cacheable |

These were verified against `params_for_simplepir`, `query_modulus_bits`,
`serialized_packing_keys_len`, `published_c1_len` and `response_body_len` at the pinned
revision.

**The asymmetry is the story.** The response is 10 KiB and constant; the request is 252
KiB and grows linearly in `db_rows`. Scaled out:

| Positions | `logical_rows` | `query_bits` | Request | Response |
| ---: | ---: | ---: | ---: | ---: |
| 136 K (today) | 32,768 | 42 | 252 KiB | 10 KiB |
| 50 M | 8,388,608 | 46 | **46 MiB** | 10 KiB |

A 46 MiB upload per memo lookup is not a mobile-viable design. Long before the pool
reaches that size the upload needs attention — amortizing packing keys across queries,
a two-dimensional layout, or a different query construction. Note this is a property of
the SimplePIR first dimension, not of row sharding: distributing the database does not
change it.

### 8.2 Measured

From the coordinator host, 2026-09-02: single-query evaluation latency ~50 ms. Positions
0, 65,535, 65,536, 131,071, 131,072 and the last populated position returned
byte-for-byte matches against the canonical record journal. A dummy query used the same
endpoint and produced the same response shape. Worker 1 was restarted after publication;
the next generation reloaded both sealed database/CRS pairs with unchanged digests and a
query against position zero succeeded. A post-apply Terraform plan reported no drift.

From a developer laptop over the public internet against
`https://memo-pir.167.99.42.60.sslip.io`:

| Measurement | Value |
| --- | ---: |
| TLS connection setup + one `GET /memo/metadata` | ~0.72 s |
| `GET /memo/public-params` (28 KiB) | ~1.0 s |
| Full `memo-pir-cli query <position>` invocation | 2.9–3.6 s |
| Full `memo-pir-cli dummy` invocation | ~2.9 s |

The CLI figure bundles four HTTP requests, four TLS handshakes, client setup, and the
252 KiB upload, so it is a cold-start end-to-end number and not a per-query latency. With
~230 ms round-trip and ~50 ms of server evaluation, the remainder is connection setup and
upload — consistent with §8.1: the upload dominates.

### 8.3 Targets, not yet met as gates

- Build or incrementally publish the current snapshot within one 75-second block interval
  on the documented production server class.
- p95 end-to-end query latency below 2 seconds on a documented mobile client and network
  profile.

PIR preprocessing memory, steady-state memory, coordinator CPU under load, and mobile
client decode cost are **unmeasured**. Benchmarks must use the full finalized pool, not a
recent window.

---

## 9. How wallets use it

### 9.1 Pending state

The wallet already persists an Ironwood received note, its commitment-tree position, key
scope, `rho`, and memo. A NULL memo means retrieval is pending. A successfully decrypted
empty memo is stored using the existing non-NULL empty-memo encoding (`0xF6`) so it is
not retried forever. Pending state survives restart. Queue order is deterministic —
oldest eligible position first — and several pending notes in the same row are coalesced
and may all be satisfied by one response.

Conceptually the wallet needs two operations:

```text
list_pending_finalized_ironwood_memos(snapshot) -> [PendingMemo]
store_authenticated_memo(note_id, position, memo) -> atomic result
```

### 9.2 One query per completed eligible scan batch

After each completed compact-scan batch that includes post-activation chain data, the
wallet consumes exactly one memo-query slot:

1. Load the latest locally acceptable finalized snapshot — accept it only if its anchor
   height and hash agree with the locally scanned chain.
2. Select the oldest pending memo whose position is below the snapshot's tree size.
3. If one exists, query its row. Otherwise draw a uniformly random row in
   `[0, logical_rows)` from the OS CSPRNG and issue a dummy query.
4. Process the response through the same parsing and decryption path either way.
5. Issue no further query for this batch — regardless of success, failure, how many notes
   are pending, or how many records decrypt.

Overflow stays durable and drains on later batches or later sessions. There is no
result-dependent tail, burst, or user-triggered extra request. A session that completes no
eligible batch sends no query, so the design preserves the public scan-batch cadence
rather than trying to hide it. Raising the per-batch slot count later is a protocol
version change for every wallet at once, not a local tuning knob.

The client never selects an epoch by the target's age and never sends a segment number.
Either would leak a coarse range containing the note.

### 9.3 Accepting a memo

1. Verify schema, network, generation, geometry, and the exact response length.
2. Verify the snapshot anchor against the local chain view and that each candidate
   position is below the tree size.
3. Extract the 792-byte record from its expected slot.
4. Parse `ephemeralKey` with the canonical Ironwood parser; reject invalid encodings.
5. Run the existing full Ironwood note decryption with `IronwoodDomain`, the complete
   580-byte ciphertext, the locally selected incoming viewing key scope, and the stored
   action context including `rho`.
6. Require authenticated decryption to succeed. No partial or unauthenticated memo
   parsing.
7. Compare the decrypted note against the locally stored note field by field — value,
   recipient/diversifier, randomness, and every other consensus-relevant field the library
   exposes.
8. Only then, atomically persist the memo for that exact note.

Never create a note from a PIR response, never change a note's value or recipient, and
never accept a memo because AEAD verification alone succeeded. Records in the row for
which the wallet has no pending note are ignored after constant-shape parsing; the wallet
does not trial-decrypt row contents against all its keys.

### 9.4 Failure, and the rule about falling back

Timeout, invalid metadata, malformed response, decryption failure, note mismatch, and
server unavailability all produce the same result: write no memo, leave the item pending,
continue compact sync, and make no immediate retry or cadence change.

Errors may be recorded locally as one non-sensitive aggregate category. Logs and telemetry
must never contain the target txid, position, row, slot, decrypted plaintext, viewing key,
or any distinction that could become a remote decryption oracle.

**There is no `GetTransaction(txid)` fallback for the covered class** — not after timeout,
not after validation failure, not after a backlog. The fallback would disclose exactly the
fact the feature protects.

### 9.5 Removing the exact-txid request

The feature only improves privacy if the wallet stops making the corresponding call. The
covered class is:

```text
MemoPirOnly:
  has Ironwood shielded actions; and
  has no Sapling component; and
  has no pre-Ironwood Orchard component; and
  has no transparent inputs or outputs.
```

For such a transaction: compact scanning stays authoritative for received notes and
spends; received notes with NULL memos become position-indexed PIR work; a spend-only
transaction creates no task; the scanner does not enqueue `Enhancement(txid)`; and the
dispatcher does not call `GetTransaction(txid)`. Eligible enhancement rows left by older
app versions are quarantined before dispatch when local evidence is sufficient to classify
them.

Mixed-pool, Sapling, pre-Ironwood Orchard and transparent transactions keep the legacy
path and remain a known leak.

**The classification must fail closed.** Ambiguity must not be silently labelled
`MemoPirOnly`, and code must never infer the absence of transparent or other-pool
components from compact Ironwood data when the compact protocol cannot establish it. It
is not yet documented which compact-block and wallet database fields prove membership
without fetching the transaction. If it cannot be proved, version 1 narrows the covered
class — it does not weaken the rule.

### 9.6 Status of the client

No wallet client exists yet. `memo-pir-cli` (`memo/memo-pir/src/client.rs`) is the
reference implementation: it fetches metadata and public parameters, generates a real or
dummy global query, validates generation bindings, and decodes one 792-byte record from
the returned row. Wallet integration would follow the shape described in
[`pir_wallet_integration.md`](pir_wallet_integration.md).

---

## 10. Future extensions: outgoing decryption and nullifiers

Sketches, not committed designs. Both are recorded here mainly for their cost.

A fuller proposal now exists in [`pir_deployment_architecture.md`](pir_deployment_architecture.md):
it widens this record for DAG-sync (including the second-column trade below), keeps the
nullifier service separate on a cold/warm schedule, and shares ingestion, sharding and
deployment across all three services. The sketches below are retained as the cost analysis
that proposal starts from.

### 10.1 Outgoing recovery and full decryption

*Resolved: the record now carries `cv_net`, `outCiphertext`, `nf`, `txid` and height
(§4.1), taking the first shape below with the extra fields folded in at once. The
analysis is kept for the cost it documents.*

Recovering sent memos and recipients after a seed restore needs `cv_net` (32 B) and
`outCiphertext` (80 B) in addition to what the memo-only record stored. That takes the record from
612 to 724 bytes — about 18% more bytes per position, and ~33.7 GiB at 50 M positions.

Two shapes, with different costs:

- **Widen the record.** Eight 724-byte records give a 5,792-byte row. That pushes
  `instances` from 2 to 3, so `db_cols` and the response both grow by half, and every
  existing shard artifact is invalidated — a full rebuild of the pool, not an append.
- **A second parallel column** keyed by the same position, holding only
  `cv_net ‖ outCiphertext`. Sealed memo shards stay valid, and only wallets that need
  outgoing recovery pay for it. But a second column is a second query, and that breaks
  the one-query-per-batch schedule in §9.2 — the schedule can only change for every
  wallet at once, or the difference itself becomes a fingerprint.

The second is the better trade, and its cost is a protocol-version change to the query
schedule rather than a database rebuild.

### 10.2 Nullifiers

This repository already runs a nullifier PIR service (`nullifier/spend-server`, described
in [`pir_wallet_integration.md`](pir_wallet_integration.md)) as a startup spendability
gate. It answers a different question over a different index — set membership over
nullifiers, not lookup by position — and it runs on a different schedule, because
spentness needs freshness that a 10-confirmation memo snapshot does not.

A per-position "spent" column in the memo database would let one position-indexed query
answer "is the note at position `p` spent", but only for notes the wallet already found,
and it would force one record layout to serve two freshness requirements. The likelier
shape is that the two services stay separate and share ingestion and deployment — the
same Zakura archive, the same worker fleet, the same generation discipline — rather than
sharing a record.

Whatever is built, two rules hold: neither extension may overload the memo record, and
neither may reintroduce a result-dependent request count.

Also still open, as separate projects: private mined-transaction retrieval for full
history and mixed pools; a rolling mempool PIR or private-set-membership service to
replace `GetStatus(txid)`; transparent discovery without sending addresses to
lightwalletd; privacy for the iOS background preparation path; and analysis of
compact-scan range scheduling and connection-level metadata.

---

## 11. What is verified, and what is not

| Verified in the POC | Still required before wallet traffic |
| --- | --- |
| Continuous ingestion from Ironwood activation with per-block tree-size cross-check | End-to-end sharded-versus-monolithic transcript equality at the memo level |
| Boundary-position queries (0, shard edges, last populated) byte-identical to the journal | Proof that worker selection, timing, errors, caching and response sizes do not reveal a target shard |
| Dummy query with identical endpoint and response shape | Privacy transcript tests: zero / one / many pending notes must be indistinguishable |
| Sealed artifact reload after worker restart with unchanged digests | Negative-case matrix: mutated ciphertext, wrong position, stale anchor, wrong network, truncated and oversized responses, padding records |
| Atomic generation staging and swap; two-generation retention | Proof that every `MemoPirOnly` case makes zero `GetTransaction(txid)` calls, including legacy queued rows |
| Append-stable shard placement; adding a worker moves no sealed shard | Production performance: preprocessing and steady-state memory, publish-within-one-block, mobile p95 |
| Terraform apply with no post-apply drift | Independent cryptographic review of the pinned `e875404` revision and its parameters for a 6,336-byte row at each supported capacity |
| Library-level algebra equivalence and malformed-contribution rejection | The `MemoPirOnly` classification proof (§9.5) |

Rollout order follows that split: verify ingestion and publication; validate correctness,
negative cases and privacy transcripts; ship wallet retrieval while retaining metrics that
can prove no exact-txid request occurs for the covered class; only then suppress legacy
queued enhancement rows; and expand the covered class only through a separately reviewed
protocol revision.

Operationally: compact sync and wallet startup never block on the PIR service; snapshot
builds and swaps are atomic; health is published separately from query results and client
retry cadence does not react to it; metrics stay aggregate and never carry opaque-query
fingerprints or anything derived from a connection-to-position mapping; and resource
limits cover request length, deserialization depth, allocation, concurrency and evaluation
time.

---

## 12. References

- [Zcash light wallet protocol](https://github.com/zcash/lightwallet-protocol)
- [ZIP 244: Transaction ID Digest](https://zips.z.cash/zip-0244)
- [YPIR paper](https://www.usenix.org/conference/usenixsecurity24/presentation/menon)
- [`pir_wallet_integration.md`](pir_wallet_integration.md) — the sibling nullifier and
  witness PIR subsystems and their wallet integration
- [`roman_notes.md`](roman_notes.md) — superseded txid-directory and cuckoo-hashing design
- [`memo/README.md`](../memo/README.md) — build, run, and operator notes for the POC
- `infra/digitalocean/memo-poc` — the deployment definition
