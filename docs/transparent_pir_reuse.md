# Four-way reuse in incremental transparent-history sync

Date: 2026-09-04. **Four-way reuse materially improves the measured wallet
workloads, but does not make the large-history case cheaper than compact
scanning.** The experiment is integrated into the adaptive
[incremental harness](transparent_pir_incremental.md); it is not a Vizor or
production-service deployment.

## What changed

The new [row-file harness](../tools/transparent-pir-reuse-bench/src/main.rs) uses
the sibling `ipir-sp` demo's feature-gated `QueryPool` and `ReusableBatch` APIs.
The cryptographic construction and production dependencies were not changed.
The sibling working tree was left intact. Its exact experimental source and
per-file digests are preserved with this report, since a base commit alone does
not identify the uncommitted experiment.

Each batch creates a fresh secret and evaluation keys and consumes at most four
independent public-matrix slots. A slot is never used for a different new query
under that secret. Live retries send the original query bytes; process restart
discards the private batch and starts fresh. Directory and page stages have
separate batches. The durable incremental journal contains decoded work, not
batch secrets or resumable slot counters.

The adapter tracks public decoding-data cache entries by executable, immutable
generation, table and slot. Each new public slot is charged once when caching
is enabled. A generation change, table change, or cache eviction requires the
corresponding public data again. This cache is shared public material, not a
long-lived client evaluation-key cache.

The automatic choice uses actual query count, key serialization size and cached
public slots. It selects reuse only when saved key bytes exceed the additional
public decoding download; ties use fresh queries. For N queries, four-way reuse
uploads `ceil(N/4)` key pairs. It does not pretend a partly used final batch was
full or share one key across an unlimited number of queries.

For the current directory, two cold queries already justify reuse. For the
larger history pages, two or three cold queries do not; four do. With all public
data cached, two queries justify reuse for either table. A single lookup stays
fresh. The current byte-choice rule does not optimize CPU or server memory.

## Same-workload results

Fresh and reuse modes were executed on the same dedicated 8-vCPU Xeon Platinum
8358 Linux host, using the same resolved mainnet day, script groups, directory,
history pages and ledger oracle. Each mode has one cold and two warm repetitions
per workload. Every recovered row and event matched expected data.

Warm means the public decoding data is cached. Each repeat resets the synthetic
wallet checkpoint and replays the same job with new private secrets; it does not
represent a wallet redundantly resyncing an already-completed range. Public
filters and the manifest are charged in every repetition.

| Workload | Fresh cold | Reuse cold | Fresh warm | Reuse warm |
|---|---:|---:|---:|---:|
| 100 unchanged scripts | 174,379 B | 174,379 B | 174,379 B | 174,379 B |
| One active script | 300,331 B | 300,331 B | 285,995 B | 285,995 B |
| Ten small histories | 1,304,875 B | **745,771 B** | 1,290,539 B | **688,427 B** |
| Largest history, whole day | 6,976,811 B | **4,009,259 B** | 6,890,795 B | **3,708,203 B** |
| Largest history, half-day checkpoint | 6,976,811 B | **4,009,259 B** | 6,890,795 B | **3,708,203 B** |

The ten-history job improves by 42.8% cold and 46.7% warm relative to fresh keys.
The large-history job improves by 42.5% cold and 46.2% warm. These are reductions
against the fresh-key PIR path, not against compact scanning.

The compact increment remains **2,888,097 bytes** for the full day and
**1,423,042 bytes** for the half-day suffix. Sparse jobs still pass the sample
50% byte-savings screen. The large job still fails: reuse is about 1.39× compact
cold and 1.28× warm for the day, and about 2.82× cold / 2.61× warm for the suffix.
No production-wallet percentile claim follows from these synthetic groupings.

### Why the large case lands at 4.01 MB

Its directory needs one fresh query. Its 50 history pages need 13 four-way key
batches: twelve full batches plus one two-query batch. Two slots are unused.

| Component | Cold bytes |
|---|---:|
| Public manifest and activity filters | 174,379 |
| Directory: one key, selector and response | 111,616 |
| History-page keys: 13 × 86,016 | 1,118,208 |
| History-page selectors: 50 × 20,480 | 1,024,000 |
| History-page responses: 50 × 25,600 | 1,280,000 |
| Public decoding data: one directory set + four page sets | 301,056 |
| **Total** | **4,009,259** |

This replaces 50 page-key uploads with 13, not with one. The demo's smaller
key-dominated shape has a higher key fraction than these larger history pages,
so its 60.6% saving cannot be transferred directly.

## Server resource tradeoff

These are measured retained packing-polynomial payloads for our actual table
geometries, not the demo's full-nullifier-snapshot geometry:

| Table | Encoded shape | Fresh: one set | Reuse: four sets | Public decoding bytes/set |
|---|---|---:|---:|---:|
| Directory | 4,096 × 2,048 | 100,630,528 B | 402,522,112 B | 14,336 |
| History pages | 4,096 × 10,240 | 503,152,640 B | 2,012,610,560 B | 71,680 |

Keeping all four sets for both tables totals **2,415,132,672 bytes (2.25 GiB)**
of packing payload for one generation. Two retained generations would therefore
need about 4.50 GiB of that payload alone. This is shared per generation/table,
not multiplied by the number of client wallets. It excludes databases, fixed
key-image caches, client data, allocator overhead and preprocessing transients.

The harness instantiates one table per process. Its sampled combined client/
server RSS is not a measurement of both tables concurrently served, a server
peak, or mobile client memory. The preserved summary records preparation and
sampled RSS ranges for each geometry. Concurrent publication/retention and load
remain unmeasured; this is not a production capacity pass.

Client generation/serialization is measured with batch-key generation charged
once per batch. Timing samples and raw stages are preserved, but fresh and reuse
runs were not interleaved. Every process rebuilds preprocessing and public data;
warm caching changes byte accounting, not the measured implementation's runtime
path. No server throughput or warm mobile latency improvement is claimed.

## Failure and boundary checks

- **Partial batches:** ten directory lookups use three key uploads; fifty pages
  use thirteen. Empty slots are neither billed as useful queries nor reused by
  another secret-preserving invocation.
- **Budget/restart:** the large job pauses five times under a ten-query work
  budget, retains completed rows, and starts fresh batches for remaining work.
  It still recovers exactly 9,152 events with 51 useful queries. Discarded partial
  batches raise reuse traffic from 4,009,259 to **4,267,307 bytes**. Public decoding
  data remains cached; private batch state does not.
- **Response loss across process lifetime:** a completed response is discarded
  before journaling. Retrying starts a fresh process/batch, charges the second
  key upload and query, and recovers the same two-event ledger. Public data is
  retained; total traffic is **411,947 bytes**.
- **Live retry:** a separate real-row probe replays exactly the same serialized
  query within a live batch, with the same slot and no second key upload. It
  verifies ring-boundary rows, a partial second batch, and a fresh-process replay.
- **Policy boundaries:** real page queries verify that a single directory lookup
  and two cold history-page lookups remain fresh, while two warm page lookups
  use reuse. Public-cache tests cover slot expansion, generation/table separation
  and eviction.
- **Underlying demo regressions:** the cancellation-attack regression,
  4/8/16-slot allocation/exhaustion and fixed public vector tests, and end-to-end
  reused decoding across batches and ring boundaries were rerun successfully.

All existing incremental state-machine tests remain applicable. This work does
not resolve the demo's conditional security assumption, authenticate malicious
server responses, or implement HTTP bindings. Production must bind setup,
parameters, generation, batch key and slot; API discipline and successful
plaintext recovery are not a cryptographic security proof.

## Recommendation

Retain four-way reuse as a candidate for the sparse-sync adapter, with the
cold/warm byte-aware choice and fresh-secret restart behavior. Do not advance to
eight sets without a workload benefit that pays for the additional retained
state. The next algorithm experiment should reduce unnecessary old-page retrieval
or improve private page navigation; key reuse alone did not fix catch-up.

Vizor shadow-mode/device measurements, transport binding and specialist security
review remain necessary before production adoption. Neither wallet balances nor
production retrieval were changed in this experiment.

## Evidence and reproduction

[Summary](transparent-pir-evaluation/reuse4/summary.json),
[method](transparent-pir-evaluation/reuse4/method.json), per-query JSON, source
archives and SHA256SUMS are under `docs/transparent-pir-evaluation/reuse4/`.
The dependency archive expands to a sibling `ipir-sp` directory. The new crate's
relative path dependency is intentional: a git revision alone would omit the
uncommitted feature. The existing production crate remains pinned as before.

```sh
cargo build --release --locked --manifest-path tools/transparent-pir-reuse-bench/Cargo.toml
PIR_REUSE_MODE=auto RAYON_NUM_THREADS=8 python3 tools/transparent_pir_incremental_run.py \
  --sample docs/transparent-pir-evaluation/mainnet-study/mainnet-day.jsonl.gz \
  --binary tools/transparent-pir-reuse-bench/target/release/transparent-pir-reuse-bench \
  --output data/reuse-reproduction --runs 3 --public-cache
# Repeat with PIR_REUSE_MODE=fresh and a separate output directory for the baseline.
python3 tools/transparent_pir_reuse_summary.py --check
python3 -m unittest discover -s tools -p 'test_transparent_pir_*.py' -v
```

The recorded build additionally used `RUSTFLAGS="-C target-cpu=native"` and a
shared target directory. The probe tool accepts `--binary`, `--generation` and
`--output` to repeat retry and policy checks on generated row files. All measured
payloads exclude HTTP/TLS, routing envelopes and cover traffic. No device target
has yet been specified.
