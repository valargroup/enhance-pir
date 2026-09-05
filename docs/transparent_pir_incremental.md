# Incremental transparent-history prototype

Date: 2026-09-04. **The integrated prototype passes the sample byte screen for
sparse updates; heavy histories and catch-up still fail. Production remains on
hold.** This advances the [mainnet study](transparent_pir_mainnet_study.md) from
separate component measurements to an adaptive client execution.

The target remains any Zcash wallet, with Vizor as the first adapter. No Vizor
application code, production service, or wallet data was changed in this step.

The [four-way reuse follow-up](transparent_pir_reuse.md) now measures the same
workloads with bounded key amortization and public-data caching. The original
fresh-key figures below are preserved as the earlier baseline.

## Implemented behavior

The [incremental engine](../tools/transparent_pir_incremental.py) starts with a
known script scope, accepted chain checkpoint, and a fixture of known outputs.
It downloads public whole-range filters, privately retrieves directory buckets
for filter matches, discovers page locators from decoded directory responses,
and privately retrieves the required history pages. There are no oracle-supplied
page requests in this path.

A generation binds network, schema, start/parent, end/anchor, filter digest,
directory/page geometry and table digests. Its identifier is a digest of the
canonical manifest. Publication is immutable; reopening a generation with
changed contents is rejected. The transport checks table size and digest before
serving its rows. These are consistency checks under the complete-indexer
assumption, not authenticated completeness proofs.

The directory has two inline events and 3,584-byte rows. History rows are sized
independently at 17,920 bytes, with 186 events per page. Page headers carry exact
script identity, ordinal/count, event count, and minimum/maximum event heights.
This version fetches every history page for a matching script in the generation;
it learns and validates page bounds after retrieval, rather than relying on a
free navigation index to skip pages.

Per-block filters include receives and consumed-prevout scripts, with a target
false-positive probability of 10^-6. Their binary headers are 44 bytes and bind
height and block hash; the manifest supplies the network/generation context.
The whole filter bundle and manifest are charged. The last two blocks are marked
as the fresh tail; publishing a larger immutable generation moves that boundary.
This models tail sealing and consistency, not a measured live publication SLA.

Pending directory/page rows are journaled separately from committed events and
coverage. Journal replacement uses fsync and atomic rename. Query budgets leave
coverage unchanged; restart reads the journal and resumes remaining work.
Coverage and the resulting UTXO fixture advance only after complete event
validation. Missing receives, malformed pages, unsupported scopes, missing
filters, stale anchors, and mixed generations fail closed. There is no public
address/outpoint lookup fallback.

Receive/spend application checks stable identities, outpoint references, values,
and exact script matches. Coinbase identity is retained for a future wallet
adapter's maturity policy; this engine does not declare outputs spendable.
Changing the script scope cannot inherit pending coverage. Earlier discovery for
new/imported scripts remains a separate wallet responsibility.

Reorg handling requires an accepted ancestor that also appears in the client's
retained chain. It rewinds events and coverage, reconstructs the output fixture,
and discards pending rows before accepting the replacement generation. Reorgs
before the retained base require an earlier checkpoint. A pending generation
cannot silently switch to another publication; finish the retained generation
or explicitly rewind to the committed checkpoint and restart.

## Integrated measurements

The [runner](../tools/transparent_pir_incremental_run.py) executed real, fresh,
serialized IPIR queries on the existing 8-vCPU Xeon Platinum 8358 benchmark host.
It used the same pinned backend as the earlier study. The Python client decides
page requests only after decoding directory results. Each ordinary workload ran
three times, with identical byte totals and exact event/UTXO-fixture equality
against an independent traversal of archive transaction dictionaries.

Coverage is the prior study's 1,152 blocks, heights 3,470,268–3,471,419. Wallet
script groupings and starting output sets are synthetic. The starting fixture
contains pre-window outputs referenced by these events, not the complete real
balances of the chosen mainnet addresses. All source events are public chain
data; no wallet secrets or private traces were used.

| Workload | Recovered events | Private queries | Total application bytes | Compact increment, same coverage | 50% byte screen |
|---|---:|---:|---:|---:|---|
| 100 unchanged scripts | 0 | 0 | 174,379 | 2,888,097 | Pass: 94.0% savings |
| One active script | 2 | 1 | 300,331 | 2,888,097 | Pass: 89.6% savings |
| Ten median sample histories | 20 | 10 | 1,304,875 | 2,888,097 | Pass: 54.8% savings |
| Largest sample history | 9,152 | 51 | 6,976,811 | 2,888,097 | Fail: 2.42× baseline |
| Largest history, half-day checkpoint | 8,576 | 51 | 6,976,811 | 1,423,042 | Fail: 4.90× baseline |

Totals include the serialized public manifest/filter bundle, PIR uploads and
responses, and published setup for each instantiated table batch. One active
script costs 174,379 public bytes + 14,336 setup bytes + 106,496 upload bytes +
5,120 response bytes. The ten-history case coalesces directory rows before
querying. Filter false positives, when present, flow through the same exact-key
lookup and accounting; the dedicated false-positive test confirms they do not
create address-use events.

The lower filter overhead than the earlier 224-KB estimate comes from this
prototype's actual 44-byte filter header and shared manifest, replacing that
experiment's 88-byte per-interval envelope. It is an encoding change, not a new
chain activity measurement.

The compact comparison uses the previously verified endpoint's combined-minus-
shielded application frames. The [baseline](transparent-pir-evaluation/incremental/baseline.json)
retains per-height increments, so the half-day comparison uses only its matching
suffix. The [derived summary](transparent-pir-evaluation/incremental/summary.json)
is authoritative for coverage-matched gates; the raw runner's half-day row marks
its whole-day screen inapplicable.

These are **sample application-byte gates**, not mobile or production gates.
There is no HTTP/TLS framing, request-routing envelope, network RTT, wallet scan,
or mobile peak-memory measurement. The accepted research contract permits query
count/timing leakage, so no cover traffic is charged. Local IPC passes selection
indices into a combined client/server harness; this is not a captured private
network transport or an adversarial-service privacy test.

Each batch reconstructs server preprocessing. Saved wall times include that
work, Python processing, and files; they must not be presented as wallet latency.
No warm setup cache is modeled: resumed batches conservatively pay setup again.
Server-side file consistency checks are covered by the final recovery tests;
the preserved mainnet timing run predates that additional file-digest check.

## Recovery evidence

- **Budget interruption on real mainnet data:** the 9,152-event history paused
  five times under a ten-query budget, then completed with exactly 51 queries.
  No partial run advanced committed coverage. Additional batch setup raised
  total bytes to 7,335,211; completed rows were retained rather than queried again.
- **Lost response with real PIR:** a measured response was discarded before
  journaling its row, and the client retried. Both queries and their setup were
  charged: 426,283 bytes total. The resulting two-event ledger was exact.
  This injects client-side response loss, not a physical packet-loss experiment.
- **Real PIR tail sealing and reorg:** a synthetic chain was synced through one
  publication, extended, rewound, and replaced with a fork containing an offline
  receive/spend pair. The final ledger matched and the orphaned outputs were gone.
- **22 deterministic state-machine tests:** cover false positives, filter and
  page corruption, missing receives, absent/unsupported scripts, changed scope,
  self-transfer, coinbase identity, budgets, retry/idempotence, stale/mixed
  generations, incorrect table binding, tail sealing, and reorg rollback.
  These use an explicitly cleartext test transport; the separate real-PIR
  recovery test validates the cryptographic retrieval path.

The [measurement results](transparent-pir-evaluation/incremental/results.json),
per-query raw measurements and executed source archive, recovery result, and
checksums are preserved in `docs/transparent-pir-evaluation/incremental/`.
The original chain fixture remains in the prior mainnet study's evidence folder.

## Decision and next boundary

**Proceed to a bounded Vizor shadow-mode adapter for sparse incremental sync,
while retaining compact scanning as the comparison and trusted production path.**
The adapter should consume accepted chain checkpoints and existing script scope,
apply this research ledger separately, and compare exact events. It must not
replace wallet discovery, spendability policy, or production retrieval merely
because these synthetic cases passed.

Before promotion, measure on a named minimum device with representative wallet
traces: cold/warm setup, actual network payloads, scan time, peak client memory,
and interrupted work. The device target is still unspecified. The 30/180-day,
full-restoration, concurrent-publication/load, and network-privacy gates remain
unmeasured. Completeness trust still needs an explicit product decision.

For catch-up, the next algorithm experiment is private page navigation or a
publicly uniform interval layout that avoids fetching old history. It must
charge navigation and additional generation setup and preserve the stated
partition-privacy boundary. The current half-day failure is evidence that this
work is necessary, not permission to silently fall back to public address
queries. This prototype is not ready for a production rollout.

## Reproduce

```sh
python3 -m unittest discover -s tools -p test_transparent_pir_incremental.py -v
cargo build --release --locked --manifest-path tools/transparent-pir-bench/Cargo.toml
RAYON_NUM_THREADS=8 python3 tools/transparent_pir_incremental_run.py \
  --sample docs/transparent-pir-evaluation/mainnet-study/mainnet-day.jsonl.gz \
  --binary tools/transparent-pir-bench/target/release/transparent-pir-bench \
  --output data/incremental-reproduction --runs 3
PYTHONPATH=tools RAYON_NUM_THREADS=8 \
  PIR_TEST_BINARY=tools/transparent-pir-bench/target/release/transparent-pir-bench \
  python3 -m unittest test_transparent_pir_incremental.RealPirRecoveryTests -v
python3 tools/transparent_pir_incremental_summary.py --check
```

The real-PIR test is skipped unless its binary is supplied. The recorded Linux
run used native CPU code generation and the existing shared target directory;
other hardware will have different timings. No new Python dependency is needed
for the incremental engine; protobuf loading in the older sample analyzer is now
lazy and remains required only for protobuf measurements.
