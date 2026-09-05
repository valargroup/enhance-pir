# Transparent-history PIR: expanded mainnet study

Date: 2026-09-04. Target: Zcash wallets generally; Vizor is the first integration.
**Decision: continue bounded protocol research; HOLD production adoption.**
The measurements support activity filters and a small inline history. They do
not establish that a lifetime private directory, complete wallet restoration,
or a mobile implementation passes the proposed product gates.

## What was measured

Two datasets answer different questions and have different fixed anchors:

| Dataset | Coverage | Result |
|---|---|---|
| Complete archived supported-address transaction index | Genesis through height **3,471,098** | **9,254,567** historically indexed addresses; **288,426,459** address–transaction associations |
| Resolved event sample | Heights **3,470,268–3,471,419**, 1,152 blocks, 86,767 seconds between endpoint timestamps | **25,008** supported receives, **45,980** supported spends, **13,394** active scripts; **zero unresolved previous outputs** |

The census reads the v28 RocksDB address-transaction index through a secondary
on the documented unpruned mainnet archive. It does not change the node service,
force a flush, or write to the primary database. Its persisted tip lagged the
live RPC tip. The anchor was checked against RPC before and after the scan.
It counts transactions associated with an address, **not individual events**;
one transaction can contain several receives or spends for the same address.
Unsupported script types are excluded from that index.

The event sample was collected with batched, rate-limited RPC reads. The first
attempt encountered a large response; the collector now splits oversized batches
and validates/resumes a complete prefix. The completed sample contains 17,700
outputs both created and spent within its window and 25 unsupported receives,
which are excluded from the supported-script directory and filters. It is a
recent activity dataset, not a lifetime restoration fixture.

Full anchors, histogram bins, cutoff heights, timestamps, and checksums are in
[the census](transparent-pir-evaluation/mainnet-study/mainnet-census.json) and
[the event measurements](transparent-pir-evaluation/mainnet-study/mainnet-day-results.json).
The compressed public event fixture is preserved alongside them.
[SHA256SUMS](transparent-pir-evaluation/mainnet-study/SHA256SUMS) fingerprints
the preserved datasets, per-run measurements, and aggregate results.

## History shape and filter results

| Distribution | p50 | p90 | p95 | p99 | Maximum |
|---|---:|---:|---:|---:|---:|
| Lifetime transactions per historical address | 2 | 11 | 60 | 544 | 1,681,694 |
| Events per active script in the recent sample | 2 | 5 | 8 | 43 | 9,152 |

At the census anchor, 12,544 addresses were active in the approximately one-day
suffix; 73,119 in 30 days; 364,380 in 180 days. Those are chain-wide address
counts, not wallet workload frequencies. Date cutoffs use timestamp search and
are approximate because block timestamps are not monotonic; the reported exact
height boundaries define the counts.

Two inline events cover **79.18%** of scripts active in the recent sample;
four cover **89.91%**; eight cover **95.12%**. These percentages cannot be applied
to lifetime histories. The very long tail requires explicit pagination, budgets,
and resumable incomplete work, even if most addresses fit inline.

Experimental Bloom filters include both output scripts and consumed prevout
scripts. All inserted-script checks passed. At target false-positive probability
10^-6, serialized sizes including the experimental 88-byte interval envelope are:

| Interval width | Intervals in sample | Filter bytes |
|---|---:|---:|
| 1 block | 1,152 | 224,380 |
| 16 blocks | 72 | 100,129 |
| 128 blocks | 9 | 68,122 |
| 1,024 blocks | 2, including a partial interval | 50,499 |

For 100 synthetic absent scripts tested against every per-block filter, observed
matches were 16 at 10^-4, two at 10^-5, and zero at 10^-6. Zero observations do
not prove zero false-positive probability. Wider sealed intervals need a fresh
tail mechanism to meet the proposed two-block freshness target.

The reconstructed transparent-only payload is **3,388,639 bytes** with identity
gRPC framing, or **3,051,046 bytes** with per-message gzip. Whole-batch gzip is
2,590,643 bytes, but that is not how Vizor currently requests this stream.
This reconstruction excludes shared metadata, fees, and HTTP/TLS overhead.

## Actual Vizor transport baseline

The inspected Vizor checkout (`635bf29b349fb66303b1797ded7bbed4dffc8052`) and
PIR PR head (`ddccc519fc1f2d3aa5abc73c919931b5e993650d`) request shielded pools
2/3/4 after Ironwood, in 100-block download batches, without enabling tonic
compression. Transparent pool 1 is an additional candidate request, not part
of that current shielded download.

The default public endpoint was queried for the same 1,152-block sample using
those pool choices. Local connections became unreliable, so the completed run
used HTTP/2 curl from the dedicated benchmark host. Each case completed once,
in 12 batches; this is payload measurement, not a latency benchmark.

| Served pool selection | Reserialized protobuf plus identity gRPC envelopes |
|---|---:|
| Vizor's shielded pools 2/3/4 | 12,013,652 B |
| Combined transparent + shielded pools 1/2/3/4 | 14,901,749 B |
| Standalone transparent pool 1 | 3,407,071 B |
| **Incremental transparent bytes for a shielded-scanning wallet** | **2,888,097 B** |

All anchors matched, and **every served transparent input and output** in both
candidate streams matched the archive fixture. The shielded element counts
also agree between shielded-only and combined requests. Response fingerprints
and endpoint/revision details are in [grpc-results.json](transparent-pir-evaluation/mainnet-study/grpc-results.json).
Serialized frame archives remain under the ignored local
`data/transparent-pir-evaluation/grpc-day-remote/` directory; the endpoint and
fixed heights allow refetching them.

These are application payload sizes reconstructed from decoded server messages,
not TLS wire bytes. Timings include SSH/CLI setup and parsing and must not be
used as Vizor sync latency. There was no app scan, device run, or gzip comparison
against the actual endpoint. The actual mixed-stream increment supersedes the
standalone reconstruction as the relevant byte baseline for Vizor.

## Real PIR directory and page experiments

The existing IPIR backend was executed on the dedicated 8-vCPU Xeon Platinum
8358 / 32-GiB benchmark host, using eight Rayon threads and native CPU codegen.
The backend revision is `e875404cef33661906ab60af236dfb327e6b28b1`; remote Rust
was 1.89.0. No production PIR service was load-tested.

The prototype uses exact P2PKH/P2SH script keys, 96-byte receive/spend events,
64-byte directory headers, and 40-byte history-page headers. A public hash salt
is retried and the table grows until every bucket fits; no entry is dropped.
The two-choice variant requires querying both possible buckets. Query counts
coalesce repeated physical rows within each synthetic workload.

Queries use fresh client randomness, serialize packing keys and query payloads,
execute the server backend, decode responses, and verify every requested row.
The replay then checks recovered event multisets against the chain fixture.
This is an in-process cryptographic benchmark: it excludes network RTT, wallet
scanning, account discovery, UI work, and any padding required to hide counts.

The first five layouts used 3,584-byte physical rows (36 events per page) and
were repeated independently three times:

| Inline events / buckets queried | Directory rows | Used history pages | Directory upload + response | Directory core p50 / p95 |
|---|---:|---:|---:|---:|
| 0 / 1 | 2,048 | 14,334 | 96,256 + 5,120 B | 21.12 / 21.86 ms |
| 2 / 1 | 4,096 | 3,715 | 106,496 + 5,120 B | 23.16 / 24.06 ms |
| 4 / 1 | 8,192 | 2,262 | 128,000 + 5,120 B | 27.62 / 28.66 ms |
| 8 / 1 | 65,536 | 1,539 | 430,080 + 5,120 B | 91.01 / 92.35 ms |
| 8 / 2 | 8,192 | 1,539 | 128,000 + 5,120 B **per bucket** | 27.61 / 28.57 ms per query |

Each table publishes 14,336 bytes of setup for this row width. The K=2 page
query has the same 106,496-byte upload and 5,120-byte response as its directory.
The reported RSS is sampled **combined client/server process memory**, not peak
mobile client memory. Server precomputation is measured separately; it must not
be interpreted as client cold setup. Query bytes exclude HTTP/TLS and do not
establish the complete protocol's setup manifest or generation-retention cost.

All six synthetic workload cases passed in all five layouts and all three runs:
unused 20 scripts, unused 100 scripts, ten small histories, ten median histories,
one very large history, and that large history with an assumed cached prefix.
For K=2, the first-run byte accounting was:

| Synthetic workload | Directory + page queries | Upload + response bytes |
|---|---:|---:|
| 20 unused scripts | 20 + 0 | 2,232,320 |
| 100 unused scripts | 100 + 0 | 11,161,600 |
| 10 median histories, 20 events total | 10 + 0 | 1,116,160 |
| Largest recent history, 9,152 events | 1 + 255 | 28,573,696 |
| Same history after assumed cached prefix, 8,576 events | 1 + 239 | 26,787,840 |

Add 14,336 bytes of cold published setup when only the directory is needed,
or 28,672 bytes when both tables are needed. The cached-prefix case assumes
known page-height bounds and locators; their discovery cost is omitted. None
of these cases constitutes HD discovery or a full seed restoration. The corpus
is synthetic script groupings over public chain events, not representative
user-wallet traces. Passing them is useful recovery evidence, not a pass for
all correctness, privacy, or mobile gates.

A second sweep held K=2 and tested physical row sizes rounded up from 4/8/16
KiB targets to the backend's 3,584-byte instance granularity. Directory and page
widths were both changed in this experiment. Each larger geometry ran once;
all six replay cases passed.

| Physical row bytes | Events/page | Largest-history page queries | Largest-history query + response bytes | Both tables' cold published setup |
|---|---:|---:|---:|---:|
| 3,584 | 36 | 255 | 28,573,696 | 28,672 B |
| 7,168 | 74 | 124 | 14,581,760 | 57,344 B |
| 10,752 | 111 | 83 | 10,225,664 | 86,016 B |
| 17,920 | 186 | 50 | 6,726,656 | 143,360 B |

Larger pages substantially help the long history, but even the largest tested
page still costs more than the whole day's 2.89-MB transparent increment. A
useful follow-up is a small directory with independently sized history pages;
this sweep does not select one global row width for both tables.

As screening arithmetic, the 3,584-byte K=2 layout plus per-block 10^-6 filters
and directory-only cold setup stays under half of the measured Vizor increment
at **at most ten physical queries** for this day. Eleven exceeds that threshold.
This excludes false positives, additional page setup, retries, HTTP/TLS, and
privacy padding. It is a budget derived from measured components, not a tested
hybrid wallet session or a production savings percentile.

The [preserved PIR summary](transparent-pir-evaluation/mainnet-study/pir-summary.json)
contains all eight geometries, separate upload/download sizes, sample counts,
p50/p90/p95/p99/max stage timings, setup measurements, and replay results.

## Conclusions and next implementation boundary

1. **Use filters as the next bounded prototype's entry point.** Directly probing
   many unused scripts is expensive even for a small recent-history table. A
   filter negative only proves absence within its covered interval under the
   stated trusted-indexer assumption; it cannot establish lifetime absence.
2. **Keep inline history small while measuring page geometry.** K=2 beats K=4
   for the measured small and median groups. K=8's coverage improvement does
   not compensate for its sparse one-choice table. Two choices improve packing
   but add lookups. This is a local result, not a production parameter choice.
3. **Solve lifetime scale before promising private restoration.** Even with
   perfect packing, 9,254,567 keys in this K=2 format need at least 711,890 rows,
   or about 2.38 GiB of plaintext directory, before collision slack, pages,
   PIR preprocessing, and retained generations. The measured K=2 directory has
   only 4,096 rows. Its timings cannot be extrapolated as a demonstrated global
   service. Publicly selecting a shard could reveal protected membership or
   access information; partitioning needs an explicit privacy design.
4. **Specify the epoch and fresh-tail protocol next.** Define anchored filter
   coverage, immutable directory/page generations, page-height bounds, resume
   tokens, and reorg handling. Test uninterrupted versus interrupted and reorged
   replay before connecting a wallet adapter. Keep the completeness/trust
   assumption explicit; a PIR decode is not proof that the index is complete.
5. **Validate the Vizor adapter on a named minimum device.** Measure actual
   incremental mixed-pool traffic, wallet scan time, end-to-end sync, client
   peak RSS, cold/warm setup, and count-padding costs. Representative 30/180-day
   wallet traces, discovery cases, and concurrent publication/load tests remain
   outstanding. The census's suffix counts do not substitute for those traces.

## Reproduction and checks

```sh
uv run --with protobuf==6.33.5 python tools/transparent_pir_sample.py \
  --sample docs/transparent-pir-evaluation/mainnet-study/mainnet-day.jsonl.gz \
  --proto docs/transparent-pir-evaluation/compact_formats.proto \
  --output /tmp/mainnet-day-results.json
python3 tools/transparent_pir_layout.py \
  --sample docs/transparent-pir-evaluation/mainnet-study/mainnet-day.jsonl.gz \
  --out-dir data/reproduction/layouts
python3 tools/transparent_pir_layout.py \
  --sample docs/transparent-pir-evaluation/mainnet-study/mainnet-day.jsonl.gz \
  --page-sweep --out-dir data/reproduction/page-layouts
cargo build --release --locked --manifest-path tools/transparent-pir-bench/Cargo.toml
RAYON_NUM_THREADS=8 python3 tools/transparent_pir_replay.py \
  --binary tools/transparent-pir-bench/target/release/transparent-pir-bench \
  --layouts data/reproduction/layouts --output data/reproduction/results
python3 tools/transparent_pir_summarize.py --check
python3 tools/transparent_pir_evaluate.py --check
```

The recorded Linux run additionally used `RUSTFLAGS="-C target-cpu=native"` and
a shared target directory. Local hardware will produce different timings.
`transparent_pir_census.py` requires the archive's v28 database and rocksdict
0.3.28; running it elsewhere is not a self-contained replay of historical data.
The collector and gRPC measurement tools have `--help` for their fixed-range
inputs. To repeat the successful endpoint measurement, use
`transparent_pir_grpc.py --ssh-host roman-ipir-bench-8vcpu --runs 1` with
`--start 3470268 --end 3471419`, an output directory, and `--proto-dir` pointing
to the pinned wallet-libraries lightwallet-protocol `walletrpc` directory.
`transparent_pir_verify_grpc.py` compares a saved combined or transparent-only
frame archive against the resolved sample. No wallet seeds, viewing keys, or private script groupings were used.

Validation includes exact decoded-row and event equality, all inserted-filter
membership checks, and rejection of a corrupted page identity, a missing page,
and a mismatched expected event count. The Rust harness passes formatting and
Clippy with warnings denied. These are research-harness checks, not a malicious
server security review or production wallet test suite.
