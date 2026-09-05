# Transparent PIR: activity filters and paged history

Status: design proposal, 2026-09-04. This document records an architecture to
evaluate; it does not specify a wire protocol or describe an implemented
transparent-address PIR service. It extends the
[transparent analysis in the Enhance PIR document](zakura_pir_enhance.md#handling-transparent).

## Goals and boundaries

Make ordinary wallets fast and inexpensive to synchronize, while letting wallets
with large transparent histories pay for additional retrieval. The intended
common case is no PIR requests when nothing changed, and a small number of
private lookups when activity occurred.

The agreed requirements are:

- Recover complete confirmed transparent wallet history, including addresses
  whose balance is now zero and outputs received and spent between wallet sessions.
- Accept shared downloads of compact recent-activity filters.
- Hide selected scripts and records inside PIR requests, while accepting leakage
  from contact with the service, request timing, and query volume.
- Bound ordinary directory entries and individual responses. Large histories use
  additional fixed-size pages rather than increasing every address's allocation.
- Preserve incomplete work on failure. Do not silently switch to public address,
  outpoint, or transaction-ID requests for work promised to this private path.

Here, complete history means the wallet's confirmed transparent receives and
spends within its recovery coverage and supported address-discovery rules. A
finite gap-limit search does not discover arbitrary derivation gaps or unknown
imported keys. Mempool activity, broadcasting, and complete raw transaction
retrieval are separate concerns. Recovering the ledger does not necessarily
recover every field needed for fees or mixed-pool transaction display.

## Architecture

Separate change detection from retrieval. A wallet downloads public activity
filters, privately retrieves a bounded directory entry for each matching script,
and privately retrieves overflow pages only when necessary.

| Component | Contents | Purpose |
|---|---|---|
| Public activity filters | Scripts receiving outputs or having outputs spent in a public block interval | Check all wallet scripts locally and skip unchanged intervals |
| Private script directory | Exact key, history summary, bounded recent events, and older-history locators | Establish absence or retrieve a small history without reading the full event database |
| Private history pages | Fixed-size pages of older receive and spend events | Retrieve large histories and cache completed historical pages |
| Recent tail | Events not yet sealed into historical pages | Publish new activity without rebuilding every old page |

These are logical responsibilities, not a commitment to four endpoints or four
independent PIR setups. The directory and recent tail may share a serving layout.

### Public activity filters

For each covered interval, include the output scripts of new outputs and the
previous-output scripts consumed by inputs. The indexer resolves spent prevouts;
filtering only new outputs would miss spends of existing wallet funds. Deduplicate
scripts within an interval and bind the filter to its block range and chain identity.

This follows the receive-and-spend coverage principle of
[BIP 158 compact block filters](https://github.com/bitcoin/bips/blob/master/bip-0158.mediawiki).
The proposal need not use Bitcoin's exact encoding parameters. Filter interval
size and false-positive probability remain measurement choices.

A correctly constructed filter has no false negatives. A negative local match
means that none of the tested scripts had an event in that interval. A positive
match triggers an exact private lookup; false positives waste work but must not
create wallet activity or mark an address as used.

Filters are downloaded for public sync intervals, independently of which scripts
match. Do not replace this with a server query containing the wallet's scripts
or an address-derived filter partition. Even a wallet holding many unchanged
UTXOs can finish a routine sync without PIR queries.

Shared bandwidth still grows with distinct affected scripts. Repeated activity
to one script can deduplicate within an interval; spam to many scripts cannot.
Filters reduce this cost rather than eliminating it.

### Bounded private directory

Key the directory by a canonical output script or a domain-separated digest of
it. Human-readable address encodings are not the storage identity. Script support
and canonical key encoding must be specified before implementation.

A directory entry conceptually contains:

```text
script key -> history summary + up to K recent events + older-history locators
```

Include enough ordering and count information to detect missing pages and locate
the suffix after a wallet checkpoint. This metadata is itself retrieved privately.
It is not exposed as a public cursor or an address-specific download URL.

Use an established keyword-to-record construction, initially evaluating a
bucketed dictionary over the existing PIR backend. Retrieve the construction's
required candidate buckets and exact-match the key locally. Empty or colliding
slots must not be mistaken for a matching entry, and bucket overflow must not
silently drop keys. Keyword PIR can address a sparse key space without allocating
one position per possible address; see
[SparsePIR](https://eprint.iacr.org/2023/466).

Pack multiple logical directory entries into each physical PIR row. A small
logical entry need not occupy an entire multi-kilobyte row. Conversely, one
logical directory lookup may require several PIR requests: do not count it as
one network operation until the dictionary construction establishes that.

Compare metadata-only entries against entries with two, four, or eight inline
events. Inline events save a second retrieval for small histories, but enlarge
the directory that every lookup processes. No inline capacity is selected yet.

### Event history and overflow

Record output creation and output spending as ordered historical events. A
receive identifies its outpoint, value, and chain location; a spend identifies
the consumed outpoint and spending transaction location. Associate both with the
relevant script. Exact record encoding and transaction metadata remain open.

An append-oriented event history preserves receive-then-spend activity even when
the wallet was offline throughout. The wallet derives current UTXOs from the
complete event sequence rather than treating the current UTXO set as history.

Keep older pages immutable within their chain generation and place recent events
in a mutable tail. Publication creates a consistent generation covering the
directory, historical page locators, recent events, and relevant filter intervals.
Sealing the tail must not duplicate or lose events.

Every history page has a fixed serving size, with padding where needed. A large
history requires additional page requests, not a variable-size response to one
PIR request. Compression may occur within pages, provided the serving size remains
fixed. Locate batches of pages without a network-dependent linked-list walk that
requires one round trip per page; the precise locator layout is an open design item.

| History size | Expected retrieval |
|---|---|
| No historical use | Directory lookup establishes absence |
| At most K events | Directory lookup supplies the complete history |
| More than K events | Directory lookup plus private overflow-page retrieval |
| Large cached history with a small new suffix | Updated directory/tail lookup and only missing pages |

All absence and completeness observations remain subject to the indexer trust
assumption described below.

## Wallet flows

### Restoration

1. Select a generation whose anchor is accepted by the wallet. Establish the
   recovery range and derive the wallet's supported initial address windows.
2. Privately look up the scripts in the historical directory. A zero current
   balance is not an absence result: previously spent outputs must be retained.
3. Apply exact history matches, advance address-discovery windows according to
   wallet rules, and repeat until the required gap limits are satisfied.
4. Retrieve all required overflow pages, reconstruct receives and spends, and
   persist coverage and resumable progress. Partial history is not a complete balance.
5. Catch up from that generation using public filters and incremental retrieval.

The directory's coverage must be explicit. It may cover all history or a declared
recovery range, but a partial range cannot answer an unqualified "ever used?" test.
Newly discovered scripts must be checked over their required historical range,
even if that range was already scanned for other scripts.

### Routine sync

1. Download the filters covering the interval after the wallet's transparent
   checkpoint, including scripts currently relevant to address discovery.
2. Match locally. With complete coverage and no matches, advance the checkpoint
   for those scripts without querying the directory or every known UTXO.
3. For matches, privately retrieve directory/tail data and the missing history
   suffix. Deduplicate events already stored and resolve exact false positives.
4. If recognition expands the derived address set, check the newly relevant
   scripts over the required earlier intervals before marking discovery complete.
5. Advance coverage only after all required matching work completes. Retain
   unresolved pages across interruption or resource limits.

A historical filter that says an address has been used is not a useful permanent
wake-up trigger: it stays positive forever. Routine filters answer "activity in
this interval?" rather than "ever used?" or "currently has funds?".

### Optional restoration membership filter

Benchmark an ever-used-script membership stage before adding it. Most derived
candidate addresses may be unused, so a smaller membership table could avoid
lookups against the larger directory. Compare three approaches:

- Query the directory directly, with no separate membership stage.
- Query a compact membership table through PIR, then query the directory on positives.
- Download and cache a public historical membership filter, then match locally.

The membership set must include all historically used scripts in its declared
coverage. A filter built only from current UTXOs misses zero-balance history.
A standard Bloom filter may require several private accesses; a blocked layout
can reduce accesses at a space/false-positive tradeoff. Tiny returned data does
not imply a tiny PIR request.

For A absent candidate scripts tested against F filters with per-test
false-positive probability p, the expected number of false-positive matches is
approximately A * F * p. Choose parameters against a wallet's full catch-up and
address-discovery workload, not one membership test in isolation.

## Correctness and privacy contract

### Coverage and generations

Bind lookups, locators, and filter coverage to the accepted chain and generation.
Do not combine a directory from one generation with pages from another unless
the publication contract explicitly proves those pages reusable. Resource limits
for setup, responses, and total work come from local policy, not server metadata.

Track transparent coverage separately from shielded scan progress. Missing
filters, unresolved page requests, unsupported script coverage, stale generations,
or malformed records must leave affected work incomplete. Validate event order,
outpoint relationships, exact keys, and page identity before applying data.

On a reorganization, rewind events and coverage to the common accepted chain,
invalidate affected filters and tail data, and reacquire affected pages. Sealed
pages are not permanently immune to reorganization. Retain cached pages only
when their chain association and continued validity are established.

### Indexer trust and completeness

The initial model assumes the indexer constructs a complete, correct history and
filter set. The PIR service is not trusted with query identities. This is not a
claim of complete protection against an actively malicious service: the concrete
PIR scheme and observable error/retry behavior require a composition review.

PIR provides query privacy, not proof that the returned records are true or that
the index contains every event. Matching the advertised anchor to an accepted
block hash does not bind the index contents to that block. A malicious indexer
can omit an event or produce a false negative filter. A transaction inclusion
proof can establish inclusion of a returned transaction, but not the absence of
another matching transaction.

Removing this trust assumption requires a separately justified authentication
and completeness mechanism. An index commitment is useful only with a trusted
or independently verified connection to the complete chain-derived index; a
self-signed index root alone does not supply that connection. Authenticated PIR
addresses record authenticity relative to an authenticated database digest; see
[Authenticated private information retrieval](https://www.usenix.org/conference/usenixsecurity23/presentation/colombo).

Under the initial assumption, a negative filter can advance transparent coverage.
It must not silently become stronger evidence than the wallet's existing
spend-verification contract. Integrating it with spendable balance requires an
explicit coverage and trust policy.

### Observable behavior and large histories

Accepting query-volume leakage lets empty wallets stop and large histories fetch
more pages. It does not guarantee address anonymity against public-chain side
information: unusual page counts or activity timing can identify likely scripts.
Even contacting PIR only after a filter match leaks activity. Coarse count padding
and scheduled retrieval are possible mitigations, with measurable costs.

Overflow locators must be queried privately. Publicly fetching a page by its
locator reveals the selection even if the directory lookup was private. Public
routing by an address-hash prefix also leaks a partition that the server can
enumerate from the chain. Keep parameter and table selection leakage explicit.

A large history is not necessarily a willing power user: someone can send many
outputs to another user's address. Bound per-session work and support resumable
retrieval without truncating history into an apparently complete balance.
Dedicated scanning or another explicitly chosen method may be more economical
for large histories; no such alternative is an automatic privacy downgrade.
Pricing and payment are outside this proposal.

## Scaling and cost model

The main savings are queries avoided by local filters, a bounded directory for
small histories, cached historical pages, and retrieval of only new events.
Overflow separates the heavy payload from ordinary directory queries, but the
operator must still store and serve that payload. Many small histories can also
make a wallet expensive, so address-level percentiles are not wallet percentiles.

Keep physical sharding behind the logical PIR interface. Workers can contribute
to a full logical query without exposing a selected address partition. Sharding
can reduce latency and distribute memory, but does not by itself reduce aggregate
database work. The existing SimplePIR-style backend scans its encoded matrix for
the first-dimension computation; batching and smaller logical tables need actual
throughput measurements rather than an assumption that work scales with reply size.

Measure client setup, query upload, response download, round trips, server CPU,
memory bandwidth, resident encoded data, publication cost, and cache behavior.
Cross-client batching may improve throughput while adding queueing latency.

### Illustrative sizes and existing measurements

The [original analysis](zakura_pir_enhance.md#handling-transparent) records the
following figures at height 3,471,419: 29,409,580 transparent outputs created,
1,375,697 spent, and 28,033,883 unspent. It also records approximately 844,457
positive-balance addresses around that time. These are historical inputs copied
from that analysis, not independently verified current measurements. The last
figure is not the number of distinct historically used scripts.

For illustration, 1,000,000 logical directory entries of 256 bytes at 80%
occupancy require 320,000,000 bytes, or 320 MB, before PIR encoding, alignment,
setup, replicas, and history pages. These are hypothetical parameters, not a
claim that an entry with four complete events fits in 256 bytes. A physical 7 KB
row for every script would be substantially more expensive than packing entries.

The existing IPIR+SP
[2026-09-02 benchmark report](https://github.com/valargroup/ipir-sp/blob/2bc1075aa72895fbaa99c83e964567aae317ea61/bench-results/2026-09-02-second-optimization-pass/REPORT.md)
reports the following wire sizes for its 28,672-by-32,768 matrix configuration:

| Component | Bytes per query |
|---|---:|
| Packing keys | 86,016 |
| First-dimension query | 150,528 |
| Response | 81,920 |
| Total | 318,464 |

That is a different workload and serving shape. These numbers establish neither
the cost of a transparent directory lookup nor a universal lower bound. They
illustrate why a small plaintext entry may still have substantial cryptographic
overhead, and why avoiding whole requests can matter more than shaving event bytes.
The report's server measurements use an eight-core Xeon Platinum 8358; they are
not mobile end-to-end measurements.

## Evaluation before selecting parameters

First collect distinct historically used scripts; historical events per script;
events since representative wallet checkpoints; and distinct affected scripts
per interval. Record median, p90, p99, maximum, and the share fitting each candidate
inline capacity. Use representative wallet workloads as well as script counts.

Compare transparent compact scanning against the hybrid design for fresh
restoration, routine daily sync, long offline catch-up, many unused derived
addresses, small histories, and large or adversarial histories. Include compressed
wire bytes and all setup/filter costs. Compare metadata-only and inline
directories, page sizes, filter intervals and error rates, and the optional
membership stage. Benchmark the existing PIR backend before selecting another.

The correctness scenarios for a prototype are:

- An unused wallet, including exact rejection of filter false positives.
- A previously used address with zero balance, and an output received and spent
  entirely between wallet sessions.
- Histories just below, at, and above inline and page boundaries; colliding keys;
  and bucket overflow without lost records.
- Out-of-order receive/spend discovery, gap-limit advancement, imported keys,
  and newly relevant scripts whose activity predates the current checkpoint.
- Interrupted pagination, retries, duplicate delivery, tail sealing, and generation
  changes without missed or duplicated events.
- Reorganizations affecting both recent data and previously cached pages.
- Missing filters, malformed pages, resource limits, and unavailable service,
  with incomplete coverage preserved and no public-query fallback.
- Network traces confirming that scripts, selected buckets, and page locators
  are not exposed outside PIR, while documenting accepted timing/volume leakage.

Publication cadence, dictionary construction, exact record schema, inline
capacity, page locators and size, filter encoding, padding policy, and service
economics remain open. The evaluation should select them before a protocol is
specified; this document deliberately makes no latency or percentile guarantee.

## Relationship to Enhance PIR and compact scanning

Ironwood Enhance PIR supplies shielded fields absent from compact blocks. This
proposal supplies transparent discovery and history through a separate path;
neither component alone resolves all mixed-pool transaction requirements.
The existing outpoint-keyed transparent-spend PIR is a narrower capability. A
complete event history with current coverage could make repeated spend lookups
redundant, but only after the wallet explicitly accepts the corresponding
coverage/trust contract. This proposal does not change its current behavior.

Transparent compact scanning remains the near-term implementation baseline for
Vizor/Zakura alongside Enhance PIR. It shares downloads across clients and avoids
address-result-dependent retrieval. This hybrid proposal is a possible bandwidth
alternative, with additional indexing and serving complexity and different
metadata leakage. Optional pool selection permits evaluating such an alternative
without replacing shielded compact scanning; see the
[lightwallet protocol](../librustzcash/zcash_client_backend/lightwallet-protocol/walletrpc/service.proto).

Promote transparent PIR beyond research only if measurements show a substantial
ordinary-wallet benefit after including restoration, updates, cryptographic
overhead, and sustainable server cost. Designing it does not make it a dependency
for shipping the existing Enhance PIR integration.
