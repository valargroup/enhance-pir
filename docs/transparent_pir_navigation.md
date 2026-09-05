# Private page-navigation experiment

This experiment compares retrieving every page for a matching script with private
binary search for the first page containing an event newer than the wallet
checkpoint. It uses the existing mainnet day, unchanged database generation and
four-way reuse backend. Navigation is opt-in in the research client
(`sync(..., navigation=True)`).

## Results

Without navigation, every checkpoint retrieves 51 rows (one directory and 50
pages): 4,009,259 bytes cold and 3,708,203 bytes warm. The navigation results are:

| Remaining blocks | Remaining events | Queries | Cold bytes | Warm bytes | Compact suffix bytes |
|---:|---:|---:|---:|---:|---:|
| 1,152 | 9,152 | 51 | 4,009,259 | 3,708,203 | 2,888,097 |
| 576 | 8,576 | 50 | 4,307,243 | 4,006,187 | 1,423,042 |
| 288 | 288 | 8 | 1,296,683 | 1,210,667 | 446,142 |
| 116 | 116 | 7 | 1,164,587 | 1,078,571 | 224,877 |
| 12 | 12 | 7 | 1,164,587 | 1,078,571 | 9,492 |

All 20 runs completed exact event and UTXO recovery. The result is conditional:

- With half the day remaining, only three pages are entirely old. Navigation
  saves one query overall but raises cold traffic by 7.4% and warm traffic by
  8.0% because the search probes consume separate key batches.
- With 288 blocks remaining, 47 pages are entirely old. Navigation cuts cold
  traffic by 67.7%, from 4.01 MB to 1.30 MB; it still costs 2.9 times the matching
  compact suffix. Here MB means 1,000,000 bytes.
- The shortest two catch-ups use seven queries each: a directory lookup and
  six page probes. The probes already contain all required suffix pages.
- No tested navigation case beats its compact suffix, even warm. For the last
  12 blocks, the 174,379-byte manifest/filter download alone exceeds the entire
  9,492-byte compact suffix by 18.4 times, before any PIR traffic.

This is evidence that private page skipping works, and that checkpoint age
alone does not guarantee a bandwidth win. It is not evidence that all large
transparent wallets or all private navigation designs have the same result.

[Raw results](transparent-pir-evaluation/navigation/results.json),
[validated summary](transparent-pir-evaluation/navigation/summary.json),
[method and limitations](transparent-pir-evaluation/navigation/method.json),
per-query JSON and exact measured source archive are retained with SHA256SUMS.
The summary checker reconciles serialized upload, response and setup totals
against raw batches and rejects duplicate row retrieval within a run.

## Method

The fixed snapshot covers heights 3470268–3471419. The script with 9,152 events
is replayed from five synthetic checkpoints: before the day, after 576 blocks,
after 864 blocks, after 1,036 blocks, and after 1,140 blocks. These are checkpoint
positions, not wallet-population percentiles. Starting UTXOs come from the
existing archive oracle fixture, which includes required prior outputs rather
than a real wallet's entire balance.

Each strategy gets an independent initially empty public decoding cache. Each
checkpoint runs once cold and once warm. Every completed run must match the
independent transaction-dictionary oracle for event multiset, final UTXOs, and
checkpoint before its result is recorded. Serialized query, key, response,
manifest, full filter bundle and uncached public decoding bytes are all charged.
The compact comparison uses the previously measured application-frame bytes for
only the blocks after that run's checkpoint.

Pages hold up to 186 events in chronological order, with their minimum and maximum
block heights in the encrypted row. Binary search privately retrieves a whole
page and compares its maximum height with the checkpoint. Equality goes to the
old prefix: events at the checkpoint have already been processed. Once the first
new page is located, the client fetches the suffix, reusing any pages already
retrieved during navigation. Full-day retrieval bypasses search. The experiment
does not use an inline-event shortcut or an additional compact navigation table.

Every adaptive search call starts a separate backend invocation. A single query
uses a fresh key under the existing byte-aware policy; a larger suffix fetch can
use four-way reuse. Public decoding data remains cached across calls, but secret
batch state does not. Thus the results include the loss of key amortization
across search rounds. They do not establish the cost of a persistent process
that retains an unconsumed batch across adaptive rounds.

## Correctness and privacy scope

The generation, row widths, directory pointers and cryptographic backend are
unchanged. Decoded search pages are journaled before the next search step. A
budget pause leaves coverage and UTXOs unchanged; resumption replays the search
using journaled rows. Validation checks page identity, exact expected occupancy,
height bounds, ordering among observed pages, ordering against inline events,
and exact suffix length. Tests cover repeated block heights across page
boundaries, checkpoint equality, a suffix containing only inline events,
one-query budget restarts, and corrupted search bounds.

Completeness still relies on the trusted builder's globally sorted and complete
history. Checking observed pages cannot authenticate an unobserved prefix against
a malicious indexer. This is not a new security proof. Page IDs remain private
PIR selections; query counts and adaptive round timing are observable, consistent
with the existing research contract. Padding and a production transport are not
implemented.

## Recommendation

Keep navigation experimental. It can remove most irrelevant history when the
checkpoint is late enough, but a small old prefix does not pay for adaptive
search and separate key batches. This single address/day does not establish a
production threshold for choosing the strategy.

The next experiment should compare shorter public snapshot intervals and
checkpoint-scoped filter downloads against the daily generation. Sweep several
interval sizes on this same resolved day, charge every directory query and cold
setup across all intervals needed for catch-up, and retain the independent ledger
oracle. Include both sparse wallets and this heavy script. The comparison should
also report retained server cache across concurrently served generations; smaller
intervals must not hide a multiplied cache or initialization cost.

A persistent client that consumes the four distinct slots across adaptive rounds
is a separate possible improvement. This experiment deliberately uses the
existing restart-safe subprocess boundary, so its search penalty should not be
interpreted as an intrinsic lower bound for the cryptographic scheme. A compact
private navigation index is another candidate, but its directory space, setup and
query costs must be included. Neither optimization by itself removes the full-day
public filter cost for a short catch-up.

## Reproduction

Build the reuse harness with the exact dependency archive described in
[the reuse report](transparent_pir_reuse.md). Then run:

```sh
PIR_REUSE_MODE=auto RAYON_NUM_THREADS=8 python3 tools/transparent_pir_navigation_run.py \
  --sample docs/transparent-pir-evaluation/mainnet-study/mainnet-day.jsonl.gz \
  --baseline docs/transparent-pir-evaluation/reuse4/baseline.json \
  --binary tools/transparent-pir-reuse-bench/target/release/transparent-pir-reuse-bench \
  --output data/navigation-reproduction
python3 tools/transparent_pir_navigation_summary.py docs/transparent-pir-evaluation/navigation
python3 -m unittest discover -s tools -p 'test_transparent_pir_*.py'
```

Measurements use the existing 8-vCPU, 32-GiB Xeon benchmark host. Warm refers to
public decoding byte accounting; each subprocess rebuilds server preprocessing.
No HTTP/TLS overhead, mobile runtime, fleet throughput, or real Vizor sync latency
is measured. The experiment adds no new server packing-cache sets beyond the
existing one/four-set configurations.
