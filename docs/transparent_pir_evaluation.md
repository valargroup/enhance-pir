# Transparent PIR feasibility assessment

Date: 2026-09-04. Decision: **HOLD production adoption; the first bounded mainnet
study and real directory/page experiments are measured; wallet-level gates remain open.**

The design targets Zcash wallets generally, with Vizor as the first integration
and benchmark target. Vizor-specific storage and transport behavior belongs in
an adapter, not the private retrieval protocol.

The [four-way reuse follow-up](transparent_pir_reuse.md) adds measured key
amortization, cold/warm accounting and cache costs. It improves sparse and large
PIR workloads, but the large-history cases still lose to compact scanning.

The [integrated incremental prototype](transparent_pir_incremental.md) now
executes filters, adaptive directory/page PIR, and checkpoint recovery together.
Selected sparse workloads pass the sample byte screen; heavy histories fail,
and device and production gates remain open.

The [expanded mainnet study](transparent_pir_mainnet_study.md) is the latest
measurement report. It adds a 9.25-million-address historical census, 1,152
resolved blocks, and actual serialized PIR/replay experiments. The original
sensitivity calculations below remain reference cases, not the new query sizes.

The [recovery/privacy contract](transparent_pir_contract.md) is concrete enough
to guide evaluation. Existing measurements establish substantial per-query cost,
but do not establish a transparent-history advantage over compact scanning.
This assessment separates recorded measurements, arithmetic sensitivity cases,
and missing evidence. The [mainnet pilot](transparent_pir_mainnet_pilot.md) adds
measured filter and reconstructed compact-payload sizes. It is not a completed
head-to-head wallet benchmark.

## Evidence available now

| Evidence | Recorded result | What it establishes |
|---|---|---|
| Local Enhance load-test artifact, anchor 3,471,029 | 6,389 successful queries in 300 s; 0 errors; 21.297 queries/s; 334.847 ms p50, 554.495 ms p95, 1,208.319 ms p99 | Performance of that existing Enhance run, not transparent directory/page performance or mobile latency |
| Existing IPIR+SP report, 28,672 × 32,768 matrix, 16 instances | 236,544 B upload + 81,920 B response = 318,464 B/query; 122.39 ms online server path | A measured geometry-specific reference for sensitivity calculations, excluding session setup and HTTP/TLS overhead |
| Current repository README, 32,768 × 4,096 Enhance configuration | 258,056 B upload + 10,256 B response = 268,312 B/query | A second recorded geometry; not a universal lower bound or the size of a transparent lookup |
| Historical counts copied into the design at height 3,471,419 | 29,409,580 outputs created; 1,375,697 spent; 28,033,883 unspent | Background scale only; not independently remeasured, and not a script/history distribution |

Sources: the original local `load-test-summary.json` is preserved in
[evidence.json](transparent-pir-evaluation/evidence.json), with its SHA-256;
[IPIR+SP report at a fixed commit](https://github.com/valargroup/ipir-sp/blob/2bc1075aa72895fbaa99c83e964567aae317ea61/bench-results/2026-09-02-second-optimization-pass/REPORT.md);
[repository README](../README.md); and the
[historical-input caveat](transparent_pir_design.md#illustrative-sizes-and-existing-measurements).
The IPIR report used an eight-core Xeon Platinum 8358. The saved Enhance JSON does
not record client hardware or collection timestamp; retrieval date is not run date.
The README describes a different 6,512-query run: do not combine its latency
percentiles with the 6,389-query JSON run.

The initial source inspection found no full historical-script census or
representative wallet trace. The expanded study now supplies a historical
address/transaction census; representative wallet traces remain outstanding. The initial SSH attempts used an old development
alias and the Enhance coordinator. The documented mainnet inventory subsequently
provided working access to `archive-vct-off` (`root@104.131.174.28`) and
`us-east-0` (`root@159.65.183.89`). The archive is unpruned and serves historical
blocks and spent previous outputs. A rate-limited read-only sample was collected;
no node deployment, restart, or production PIR load test was performed.

## What can be compared today

Let `S` be setup/download bytes paid in the measured session, `F` downloaded
filter bytes, `A` candidate scripts, `M` exact positive scripts after coalescing
work, `E` false-positive lookups after coalescing, `b` physical bucket requests
per directory lookup, and `P` total physical history-page requests. Let `qd` and
`qp` be measured directory and page upload-plus-download bytes, and `C` the
compressed transparent compact data needed for the same coverage.

| Approach | Incremental byte accounting | Current evidence gap |
|---|---|---|
| Transparent compact scanning | `C` | Actual compressed bytes and wallet processing time |
| Direct directory + pages | `S + A*b*qd + P*qp` | Historical script population, bucket geometry, page demand, setup |
| Filters + directory + pages | `S + F + (M+E)*b*qd + P*qp` | All direct-path gaps, plus interval filter sizes and match distribution |

Charge each actual session refresh and retry. Apply the same cache state and
coverage to all alternatives. For wallets already downloading shielded compact
blocks, measure incremental transparent bytes by comparing compressed combined
and shielded-only streams; do not charge shared headers twice. Also report
standalone transparent-stream bytes for wallets not downloading shielded blocks.
Upload and download must be reported separately before summing.

The following arithmetic uses the two recorded query geometries, with **two
bucket requests per directory lookup as a sensitivity assumption**, zero overflow
pages, and excludes setup, filters, and HTTP/TLS. It is not a prediction of the
eventual dictionary or a measured transparent wallet workload.

| Scenario | Physical requests | At 268,312 B/request | At 318,464 B/request |
|---|---:|---:|---:|
| Direct lookup of 20 candidate scripts | 40 | 10.235 MiB | 12.148 MiB |
| Direct lookup of 100 candidate scripts | 200 | 51.176 MiB | 60.742 MiB |
| Direct lookup of 1,000 candidate scripts | 2,000 | 511.765 MiB | 607.422 MiB |
| One changed script, no overflow | 2 | 0.512 MiB | 0.607 MiB |
| Ten changed scripts, no overflow | 20 | 5.118 MiB | 6.074 MiB |

An unchanged wallet can avoid PIR queries only when it already has complete
coverage and successfully checks all required new filters. It still downloads
filters and may need setup on a future positive. For an unused fresh restore,
an incremental activity filter is insufficient to establish lifetime absence.

The hybrid beats scanning in bytes only if
`S + F + (M+E)*b*qd + P*qp < C`. For the proposed 50% savings gate, replace `C`
with `0.5*C`. This identifies the measurements that matter before selecting a
PIR layout. Small plaintext directory entries do not imply small encrypted queries.

For false-positive planning, the expected number of script/filter matches is
`A*I*p` for `A` absent scripts, `I` intervals and per-test probability `p`.
This is before coalescing and need not equal physical query count. With 100
absent scripts, 1,152 filters and `p=10^-4`, it is 11.52 matches; with `p=10^-6`,
0.1152. At the 318,464-byte reference and two queries per match, these correspond
to 6.998 MiB and 0.070 MiB expected extra bytes before coalescing. The interval
count is an input to sensitivity analysis, not an observed daily activity count.

The filter must include receives **and consumed previous-output scripts**. The
[BIP 158 contents specification](https://github.com/bitcoin/bips/blob/master/bip-0158.mediawiki#contents)
is a reference for this coverage principle, not a selection of Bitcoin's encoding
parameters or a proof of completeness for the proposed service.

## Proposed go/no-go gates

These are explicit screening defaults for review. Evaluate byte/latency gates on
the same named minimum-supported mobile device and network, with at least 100
traces per ordinary workload class and three independent benchmark runs. Until
the device and trace population are identified, mark those gates unmeasured.
Report p50/p90/p95/p99/max, sample counts, and uncertainty; do not infer wallet
latency by multiplying per-query latency percentiles.

| Gate | Pass condition | Current status |
|---|---|---|
| Recovery | Exact receive/spend ledger equality with archive-derived truth at the same anchor; every contract correctness case passes | Not measured |
| Trust | Wallet owner explicitly accepts complete-indexer trust for the intended balance/spendability use, or a verified replacement is specified | Proposed research assumption only |
| Privacy | No selected script/partition/locator or protected public fallback in captured requests; count/timing leakage and malicious-service composition reviewed | Contract defined; not tested |
| Ordinary sync bandwidth | At least 50% reduction in median upload+download for daily sync and 30-day catch-up; p95 bytes no worse than compact scanning in either class | Not measured |
| Fresh restoration | Complete discovery and history; p95 bytes and latency each at most 1.25× compact baseline | Not measured |
| Mobile responsiveness | Daily-sync p95 completion at most 5 s and no more than baseline + 1 s; cold setup at most 2 s; peak incremental client RSS at most 128 MiB | Not measured |
| Server capacity | Sustain the pilot demand below with 2× headroom, within a total 8-vCPU/32-GiB serving budget; include directory/pages, concurrent retained generations and publication; no lost coverage, OOM, or growing backlog | Not measured |
| Freshness and recovery | New accepted events available within two blocks of the indexed tip; budget exhaustion, restart, reorg and pagination preserve resumable incomplete work | Not measured |

Pilot capacity normalization: 10,000 wallets × four syncs/day × 10× peak factor
= 4.630 syncs/s; require 9.259 syncs/s with headroom. This is an assumed load,
not a user forecast. At two queries/sync that is 18.519 queries/s; at 40 it is
370.370 queries/s. The existing 21.297-query/s Enhance run cannot be transferred
to a differently sized directory or used as proof that either target passes.
Measure aggregate CPU-seconds, resident bytes and egress per completed wallet
sync, including index-building/publication work separately. The serving budget
is an evaluation ceiling, not a provisioning request or dollar-cost claim.

Failure of recovery/privacy is no-go until corrected. Missing evidence means
HOLD, not pass. If performance gates fail after the bounded parameter sweep below,
retain compact scanning as the baseline and revisit the architecture. Large or
spammed histories may take longer than ordinary limits, but must resume exactly
without truncation or privacy downgrade. Do not omit them from reported results.

## Next bounded measurement

1. **Acquire fixed data.** Use an archive of the intended deployment chain,
   record network/genesis and anchor height/hash, and pin source/code revisions.
   Stream blocks from genesis through that anchor, resolving previous outputs.
   Retain an event dataset and manifest/checksum locally; do not publish wallet
   keys or private script groupings. A recent sample can characterize recent
   filters but cannot establish historical address absence or restoration cost.
2. **Produce the census.** Count distinct historically used supported scripts,
   per-script receives/spends and total history, event suffixes at 1-day/30-day/
   180-day checkpoints (chosen by block timestamps), and distinct affected scripts
   per interval. Report p50/p90/p99/max and coverage of inline capacities 0/2/4/8.
   Separate unsupported scripts and unresolved prevouts; any unresolved prevout
   makes receive-and-spend filter measurements incomplete.
3. **Replay wallet workloads.** Use consented local traces or clearly labeled
   synthetic script groupings: unused restore, small-history restore, daily sync,
   30/180-day catch-up, many unused derivations, and large/spammed history. Include
   zero-balance and offline receive-then-spend fixtures. Synthetic groups do not
   establish production wallet percentiles. Preserve account/gap discovery costs.
4. **Measure three alternatives.** Compare compact scanning, direct directory,
   and filters+directory using the existing PIR backend. Sweep inline capacities
   0/2/4/8, physical page targets 4/8/16 KiB, intervals 1/16/128/1,024 blocks, and
   false-positive targets 10^-4/10^-5/10^-6. Record the backend's actual row padding,
   bucket requests, setup and wire sizes; do not assume a requested page size is
   a supported physical geometry. Start with payload accounting to eliminate
   dominated shapes, then benchmark surviving configurations on fixed hardware.
5. **Apply the gates.** Measure cold and warm sessions, interrupted work and
   publication overlap. Select the lowest-byte candidate passing all gates;
   break ties by lower aggregate server CPU per sync. If none pass, record no-go
   with the failing workload. A separate restoration membership filter is a
   follow-up only if direct restoration lookups dominate the measured cost.

The expanded study completes the historical address-index scan and a bounded
directory/page encoding experiment. A lifetime event export, representative
wallet traces, and the minimum supported device remain required before making
product performance claims. The prototype encoding is not a production protocol;
next steps and remaining gaps are recorded in that study.

## Reproduce the arithmetic

```sh
python3 tools/transparent_pir_evaluate.py
python3 tools/transparent_pir_evaluate.py --check
```

The script reads the preserved [evidence](transparent-pir-evaluation/evidence.json)
and writes [sensitivity.json](transparent-pir-evaluation/sensitivity.json).
It performs no network access. Generated values are calculations using recorded
query sizes, not new PIR measurements. `--check` verifies that the checked-in
result is reproducible and that the source load-test totals are consistent.
