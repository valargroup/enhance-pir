# BIP 158 transparent activity filters: implementation handoff

Date: 2026-09-04. Status: implementation specification; not implemented or deployed.
Target: reusable Zcash wallet support, with Vizor as the first consumer.

## Decision and outcome

Implement deterministic per-block BIP 158 encoding for transparent activity,
plus downloads restricted to the wallet's missing block range. Keep filter
construction independent of private history retrieval: the same public filters
can support different wallet retrieval strategies.

The immediate deliverable is a tested Rust library, an adapter for the existing
research harness, and a reproducible comparison on the resolved mainnet day.
Production fleet rollout and Vizor integration are subsequent deliverables.
Do not change the experimental key-reuse construction in this work.

This is justified independently of whether private history retrieval eventually
wins for every wallet profile. Filters let the wallet check activity locally.
Current measurements used experimental Bloom filters rather than BIP 158. The measured
174,379 bytes include the daily manifest and filter envelopes as well as filter
contents; do not describe that entire number as removable filter overhead.
The twelve-block compact baseline is 9,492 bytes. Encoding improvements and
range-limited delivery must be measured separately rather than assumed.

## Read these files first

- `docs/transparent_pir_contract.md`: coverage, privacy and trusted-indexer model.
- `docs/transparent_pir_navigation.md`: latest results and short-update problem.
- `tools/transparent_pir_incremental.py`: `build_generation`, `read_targets`,
  `sync`, durable checkpoint and reorg behavior.
- `tools/transparent_pir_sample.py`: `make_filter`, the experimental Bloom baseline.
- `tools/transparent_pir_collect.py`: resolved previous-output scripts in samples.
- `tools/transparent_pir_incremental_run.py`: independent `oracle` and `verify`.
- `docs/transparent-pir-evaluation/reuse4/`: existing source archives and results.
- Applicable repository instructions and the ai-runbook cryptography review rule.

Preserve old evidence and reproducible Bloom runs. Do not overwrite the existing
`docs/transparent_pir_design.md` or modify the sibling `ipir-sp` working tree.

## 1. Define the Zcash filter profile

Use the name `zcash-transparent-basic-v1` in application metadata. This is a
proposed application profile using BIP 158's basic encoding and analogous
transparent-script inclusion rules, not an assertion of an existing Zcash
network standard or support for Bitcoin peer messages.

One filter represents exactly one accepted Zcash block. Hourly/daily objects may
bundle filters for delivery, but must not merge them into a single differently
keyed filter. Keep network identity, filter profile and block identity explicit.
Shielded note discovery remains the wallet's existing responsibility.

Build the element set directly from raw resolved block transactions:

1. Include each nonempty transparent output script, including coinbase outputs,
   except scripts whose first opcode byte is `0x6a` (`OP_RETURN`).
2. Include the nonempty script of the previous output spent by every transparent
   input, excluding the coinbase input. Use the previous output's locking script,
   not the input's unlocking script, transaction identifier, outpoint or address
   text. Resolve outputs spent within the same block too.
3. Deduplicate identical raw script bytes across the entire block before
   encoding. Do not deduplicate distinct elements merely because hashes collide.
4. Missing previous-output data is a construction error. Do not publish a partial
   filter or treat a failed lookup as an empty script.

This covers transparent inputs used while shielding funds, and transparent
outputs produced while unshielding, without putting shielded fields in filters.
Do not restrict the shared filter to the research wallet's currently supported
address types. In particular, **do not call `source_events()` to obtain filter
contents**: that helper drops unsupported scripts. The sample retains raw scripts
in `transactions[].vin[].script` and `vout[].script`; its collector already removes
coinbase inputs. Production extraction must explicitly recognize coinbase inputs.
Empty or unusual script handling needs fixtures rather than address decoding.

The existing private directory supports a narrower script set. Keep that
capability boundary explicit: broader filter coverage must not let `sync` claim
successful history recovery for wallet scripts its retrieval backend cannot serve.

Encoding contract:

- Fix `P = 19`, `M = 784931`; remove configurable Bloom probability from this profile.
- Use SipHash-2-4 and the first sixteen bytes of the block hash in its canonical
  little-endian byte representation. Split into two little-endian 64-bit keys
  for a library API that requires them.
- Map each hash by taking the upper 64 bits of its product with `N * M`.
- Serialize the element count with CompactSize, followed by the Golomb-Rice
  stream with zero bit padding. The empty filter is exactly `00`.

These encoding and inclusion choices follow [BIP 158](https://bips.dev/158/).
Represent hash bytes with named types/conversions. RPC display hexadecimal is
not the key byte order: reverse a displayed hash when converting to the specified
internal representation, and prove the conversion with test vectors. Do not
reuse the current Bloom seed's SHA-256/network-domain transformation.
Network separation belongs in the surrounding object/cache identity.

## 2. Reuse a library and keep Zcash extraction separate

Create a small library, proposed path `filters/transparent-activity`, as a workspace
member. Use a maintained BIP 158 implementation behind a thin adapter instead of
writing SipHash or a new compression implementation.

A concrete candidate inspected for this handoff is
[`bitcoin::bip158`](https://docs.rs/bitcoin/0.32.102/bitcoin/bip158/index.html).
Its generic [`GcsFilterWriter`](https://docs.rs/bitcoin/0.32.102/bitcoin/bip158/struct.GcsFilterWriter.html)
accepts explicit keys, parameters and byte elements. Evaluate that API and its
reader, pin the chosen dependency and commit the lockfile. Confirm compatibility
with this repository's Rust toolchain and intended mobile builds. The candidate
is not an assertion that an independent security review has been completed.

Use the generic element writer rather than attempting to parse a Zcash block as
a Bitcoin block or adapting Bitcoin's block-level constructor. Inspect the pinned
reader's malformed-input and early-return behavior; a convenient match API may
not validate the rest of a malformed filter.

Suggested library interfaces, not literal dependency APIs:

```rust
build_filter(block_hash: BlockHash, elements: &[ScriptBytes]) -> Result<FilterBytes>;
validate_filter(bytes: &[u8], limits: FilterLimits) -> Result<ValidatedFilter>;
match_scripts(filter: &ValidatedFilter, block_hash: BlockHash,
              wallet_scripts: &[ScriptBytes]) -> Result<Vec<usize>>;
filter_hash(bytes: &[u8]) -> FilterHash;
```

`match_scripts` returns the indices of ALL matching wallet scripts, not just a
boolean for whether any script matched. The current directory stage needs those
identities locally. Hash/sort the wallet's scripts once per block and match them
in a single filter pass where practical. Never send that list to the server.
Empty wallet input returns an empty match list after required validation.

Set explicit byte, element-count, allocation and decoding-work limits. Check
CompactSize canonicality, truncation, arithmetic overflow, cumulative values
outside the encoded range, padding and trailing data. Fully validate a downloaded
filter before using any result, including an early positive, to decide coverage.
Use format-derived count/length checks and document application caps separately;
an exceeded cap means an unsupported/incomplete update, not a negative match.
Production caps must accommodate valid Zcash blocks; test the largest observed
block and justify limits against chain constraints rather than guessing from the
median sample. Validate once and cache the validated object.

## 3. Store immutable filters and fetch only missing blocks

Separate filter storage from the daily private-directory generation. The same
block's filter should survive regrouping or rebuilding that directory.

Cache/storage key:

```
(chain identity including genesis hash, profile, block hash)
```

Maintain a replaceable height-to-hash mapping separately. Never key immutable
filter content solely by height. Store serialized filter bytes and their digest;
store build provenance separately from the bytes that determine encoding.

Introduce an injected `FilterTransport` with a local-file implementation first:

```
fetch_range(chain, profile, start_height, stop_block_hash)
    -> ordered filter records + actual serialized byte charges
```

The wallet chooses the contiguous missing range from its durable checkpoint to
an already accepted block. It does not request only the blocks that matched.
For long ranges use bounded batches; start with a maximum of 1,000 records per
batch as an application limit. Batching must not alter filter bytes.

Proposed application envelope, versioned separately from BIP 158:

- Batch: version, chain identity, profile, starting height, terminal block hash,
  record count.
- Record: height, block hash, byte length, raw BIP 158 filter bytes.
- Fix binary integer endianness and CompactSize length encoding in a checked-in
  format specification and golden envelope fixture. Keep human-readable hash
  display conversion out of binary serialization.

An HTTP or wallet-service adapter can use this same contract later. Public
requests contain only chain/profile/range information. They must never include
wallet addresses, scripts, matches or address-derived partition choices. Range
requests reveal the requested synchronization interval; this profile does not
hide that timing/coverage information.

Check exact response count, order, each height/hash against the wallet-owned
accepted chain, profile, network and terminal hash. Reject missing, duplicate,
truncated, wrong-fork or excess records. Apply byte/work limits before allocation.
Account for metadata, retries and every uncached byte actually delivered. A
cached, fully validated filter on the same accepted branch can be reused; do not
pretend it is cached when comparing initially empty clients.

Do not require a full-day manifest or full-day list of filter hashes just to fetch
a twelve-block suffix. Any needed metadata or proof material must be scoped to
that request and included in the measurements.

## 4. State clearly what verifies filter correctness

Keep the current complete, trusted-indexer assumption for this implementation.
Matching the wallet's accepted block hash binds the claimed filter location, but
does not prove its contents are complete. A digest supplied alongside a false
filter does not make it true; a negative can advance coverage only under the
explicit trust policy. Cached digests detect corruption relative to previously
accepted content, not dishonest construction.

Provide the standard double-SHA-256 filter digest. If implementing chained filter
headers, use double-SHA-256(filter_digest || previous_filter_header), with all-zero
predecessor only at genesis. For the bounded historical sample, use an explicitly
labeled fixture anchor or an externally supplied predecessor; never call a zero
anchor at height 3470268 a genesis-derived header chain.

[BIP 157](https://bips.dev/157/) specifies filter-header exchange and peer-based
verification separately from the BIP 158 encoding. Chaining headers is not by
itself a proof from Zcash consensus that the scripts are complete. Do not claim
full BIP 157 security or implement Bitcoin peer-service signaling in this slice.
Before production, document the authentic source of any trusted checkpoint and
how disagreement or dishonest omission is handled. A signature authenticates an
operator, not the completeness of that operator's filter.

## 5. Integrate without changing private retrieval behavior

Refactor `build_generation` and `read_targets` in
`tools/transparent_pir_incremental.py` to use the new filter abstraction. A small
Rust command-line adapter may connect the Python research harness to the Rust
library. Prefer batched operations instead of launching a process per script.
Keep command-line IPC costs separate from claims about eventual mobile speed.

Add explicit generation/filter format dispatch. Preserve old generation IDs and
Bloom evidence; do not reinterpret old `filters.bin` under a new format. Separate
wallet-state schema from generation/filter schema if necessary, since the current
`SCHEMA` constant is shared. Reject unknown versions. Resume pending work only
against its original generation/profile, or require an explicit safe restart.

The flow remains:

1. Obtain and fully validate every required public filter.
2. Match wallet scripts locally; union matching identities for private lookup.
3. For no matches, advance coverage only once complete filter coverage is durable
   and allowed by the trust policy.
4. For matches, run the existing private directory/page retrieval and exact
   script checks. A false positive can produce extra work, never a fabricated
   payment or balance change.
5. Commit events, unspent funds and checkpoint atomically only after required
   work completes. An interruption must not create a coverage gap.

On a chain replacement, roll back to an accepted common ancestor and invalidate
branch-dependent coverage, height mappings and pending retrieval. Old immutable
filter bytes may remain cached under their old block hashes, but cannot count
as coverage on the replacement branch. Changing the wallet's script set requires
coverage for newly introduced scripts; an old negative for a different script
set is insufficient.

Keep private directory/page geometry, key-reuse policy and navigation strategy
fixed for the primary comparison. If the changed filter format changes the
research generation ID, rebuild normally and charge public setup accordingly;
do not illegally share cache entries across generation IDs to improve results.

## 6. Required validation

### Compatibility and malformed data

- Vendor the [official basic-filter vectors](https://raw.githubusercontent.com/bitcoin/bips/master/bip-0158/testnet-19.json)
  with upstream revision, license and checksum. Verify exact bytes, key byte order,
  membership and filter hashes/headers where included. Check the generic encoding
  with Bitcoin vectors without using Bitcoin parsing for Zcash source data.
- Add pinned Zcash fixtures with displayed and internal block hashes, raw element
  lists, expected serialized filter and digest. Cross-check with an independent
  implementation such as the [reference Go implementation](https://github.com/btcsuite/btcd/tree/master/btcutil/gcs)
  pinned to a commit. Do not generate both expected and actual values with the
  same Rust function and call that an independent test.
- Empty filters, duplicate scripts, reordered elements, zero deltas/hash
  collisions, CompactSize boundaries and empty query lists.
- Truncated counts and bit streams, noncanonical counts, huge counts, long unary
  runs, overflow, invalid padding, trailing bytes and early-match malformed tails.
- Every included script matches. Do not assert that an absent script can never
  match: false positives are part of the construction.

### Zcash extraction and durable coverage

- Receive only, spend only with no matching new output, same-block receive/spend,
  coinbase receive, empty block, repeated script, unrecognized/nonstandard script,
  empty script and OP_RETURN output exclusion. Ensure a script with a later
  embedded `0x6a` byte is not incorrectly excluded.
- Missing previous output blocks publication. Tests distinguish raw filter
  extraction from the research history backend's supported-script restrictions.
- Missing middle filter, duplicate height, wrong chain/profile/hash, changed
  script scope, interruption/resume, cached old-fork filter and replacement chain.
- False positives cause exact retrieval checks, with unchanged balances when no
  real event exists. No address-revealing fallback is introduced.
- Run existing incremental/recovery tests plus integrated real-private-retrieval
  replay with the independent transaction oracle. Every case must recover the
  same events and final unspent funds as its corresponding baseline.

## 7. Measurement plan and definition of done

Use `docs/transparent-pir-evaluation/mainnet-study/mainnet-day.jsonl.gz`, covering
1,152 blocks at heights 3470268–3471419, with zero unresolved previous outputs.
Use per-block ordinary-download measurements from
`docs/transparent-pir-evaluation/reuse4/baseline.json`.

Separate these comparisons:

1. Old supported-script Bloom filters with full-day delivery (reproduce baseline).
2. New complete-script BIP 158 filters with full-day delivery.
3. The same BIP 158 filters with checkpoint-scoped delivery and real cache reuse.

For encoding-only attribution, also build Bloom and BIP 158 filters from the
IDENTICAL complete-script set; report the broader script coverage separately.
Their false-positive parameters differ slightly, so record both parameters and
actual extra private work. Do not attribute a change in coverage to compression.

Profiles: 100 unchanged addresses; one two-activity address; ten two-activity
addresses; the 9,152-activity address. Add clearly labeled constructed mixtures
of 100/1,000 mostly inactive addresses with a few active ones. Do not label these
wallet-population averages. Repeat full day, half day and suffixes of 288, 116 and
12 blocks, plus a sequence of incremental updates to measure real filter-cache
reuse. Select fixtures reproducibly, not by which ones produce favorable matches.

Report raw filter bytes, envelope/metadata bytes, actual cache downloads,
false-positive matches and resulting extra private requests, total transferred
bytes, exact ledger recovery, filter build/match time and bounded peak memory.
For the initially empty and cached cases, state precisely what is already cached.
Compare against ordinary retrieval for exactly the same block interval. Matching
on a benchmark server is not a measurement of phone battery or Vizor latency.

For a large deterministic set of absent scripts, report matches divided by the
number of script/filter tests and the resulting private-query count after
coalescing repeated matches. Use enough trials to make the observed rate useful;
zero matches in a small test is not proof of a zero false-positive rate.

Write raw results, exact source/dependency identities, a validating summary tool,
and an evidence report under new `bip158` paths. Preserve all previous artifacts.
The implementation is complete when compatibility, extraction, malformed-data,
coverage/reorg and integrated correctness tests pass, and the comparison table
can be reproduced. Bandwidth superiority for every profile is a research outcome,
not a condition that licenses changing the workload or hiding negative results.

Suggested commit sequence:

1. Library adapter, profile specification and independent encoding vectors.
2. Complete Zcash element extraction and immutable per-block storage.
3. Range transport, versioned harness integration and durable-state tests.
4. Comparative measurements, evidence validator and report.

Return a reviewable branch/diff and a concise report of changes, test commands,
measurements and limitations. Do not deploy services or enable the feature for
wallet users as part of this handoff. Production integration and security review
follow once this bounded implementation is validated.
