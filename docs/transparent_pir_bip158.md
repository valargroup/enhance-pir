# BIP 158 transparent activity filters: implementation and measurements

Date: 2026-09-05. Status: implemented, tested, and deployed as a loopback-only
service. Not enabled for any wallet.

Implements `docs/transparent_bip158_implementation_handoff.md`. Previous Bloom
evidence is untouched and still reproducible; results here live under new
`bip158` paths.

## What exists now

- `pir/transparent-filter` — the `zcash-transparent-basic-v1` profile: encoding,
  strict validation, wallet-side matching, the delivery envelope, and a
  reference client.
- `server/transparent-filter-server` — Zakura ingest, immutable per-block filter
  storage, and range delivery over HTTP.
- `tools/transparent_filter_build.py`, `tools/transparent_filter_measure.py` —
  offline construction and the comparison below.
- Filter-format dispatch in `tools/transparent_pir_incremental.py`.

## What verifies the encoding

Three independent checks, none of which compares this crate against itself:

1. **The official BIP 158 vectors** (`bip-0158/testnet-19.json`, vendored with
   its upstream revision and SHA-256). Filter bytes, membership and the header
   chain all match exactly.
2. **btcsuite/btcd's `gcs` package** generates expected filters for a
   deterministic case set, in a different language. All cases agree, including
   1,000-element filters and duplicate handling.
3. **A live mainnet cross-check over the whole day.** All 1,152 blocks were
   built by the service from raw blocks fetched from the production Zakura
   archive node, and compared against the same heights built from
   `mainnet-day.jsonl.gz`, which reached the node through an unrelated path
   (Python, verbosity-2 JSON, `scriptPubKey.hex`). **1,152 of 1,152
   byte-identical.** This exercises extraction, previous-output resolution,
   byte order and encoding together. The last two blocks were compared by
   reading them back through `GET /v1/filters/range`, so the envelope framing
   and the absence of trailing bytes are checked on the serving path as well.

A separate test asserts that building with the display-order hash does *not*
reproduce the reference filters, so the display/internal distinction is known to
be load-bearing rather than incidentally correct.

## Measurements

Source: `docs/transparent-pir-evaluation/mainnet-study/mainnet-day.jsonl.gz`,
1,152 blocks, heights 3,470,268–3,471,419, zero unresolved previous outputs.
Ordinary-retrieval bytes are the per-height figures in
`docs/transparent-pir-evaluation/reuse4/baseline.json`.

Raw results: `docs/transparent-pir-evaluation/bip158/results.json`.

| Interval | Blocks | Elements | BIP 158 raw | Bloom raw | BIP 158 delivered | Bloom delivered | Ordinary retrieval |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Full day | 1,152 | 34,062 | 91,136 | 123,004 | 138,716 | 170,670 | 2,888,097 |
| Half day | 576 | 14,226 | 38,193 | 51,426 | 61,957 | 75,228 | 1,423,042 |
| Suffix | 288 | 6,211 | 16,723 | 22,469 | 28,655 | 34,419 | 446,142 |
| Suffix | 116 | 2,278 | 6,150 | 8,246 | 11,014 | 13,122 | 224,877 |
| Suffix | 12 | 176 | 479 | 637 | 1,077 | 1,235 | 9,492 |

"Delivered" includes the envelope framing of
`docs/transparent_filter_envelope.md`, computed from the specification. Both
Bloom columns are built from the **identical** complete script set, so the
comparison between them is encoding alone.

The ordinary-retrieval column reproduces the handoff's own figures exactly
(2,888,097 for the full day, 9,492 for the twelve-block suffix), which is what
makes the ratios comparable to previously published numbers.

### The coverage change is not observable on this sample

All 13,394 distinct scripts in this day are P2PKH or P2SH. The old filters'
`supported()` predicate therefore excludes nothing here, and the
supported-script set and the complete script set **coincide**.

So the ~25% size reduction in the table is entirely encoding. It is not evidence
about the value of broader coverage, and must not be reported as such. Broader
coverage still changes what a filter can report on intervals containing other
script forms; this sample cannot quantify that, and no other sample here does
either.

The handoff cites 174,379 bytes for the old full-day Bloom delivery. This
report's 170,670 is the same filters under *this* envelope, not that one; the
difference is framing, not filter content.

### Checkpoint-scoped delivery

The suffix rows are what a wallet fetches when its checkpoint is that far
behind. Against ordinary retrieval for the same interval, delivered filters are
8.8× smaller at twelve blocks and 20.8× smaller over the full day. The twelve
block case is the weakest ratio because envelope framing is a larger share of a
small batch — the fixed 105-byte header and per-record 40-byte overhead do not
shrink with the filters.

### False positives

| Profile | Scripts | Interval | Blocks with real activity | Matched | Extra private block requests |
| --- | ---: | --- | ---: | ---: | ---: |
| 100 unchanged | 100 | full day | 0 | 0 | 0 |
| One two-activity | 1 | full day | 2 | 2 | 0 |
| Ten two-activity | 10 | full day | 19 | 19 | 0 |
| Busiest address | 1 | full day | 1,152 | 1,152 | 0 |
| Mixture, mostly inactive | 100 | full day | 6 | 6 | 0 |
| Mixture, mostly inactive | 1,000 | full day | 10 | 12 | 2 |

Every real activity is found in every profile and interval. False positives
appear only at 1,000-script scale: two extra block requests across a full day.

A separate probe tested 10,000 deterministic absent scripts against 12 filters —
120,000 script/filter tests — and observed **zero** matches. The expected rate is
1/M = 1.27e-6, so about 0.15 matches were expected; zero is consistent with that
and is **not** evidence of a zero false-positive rate. False positives are part
of the construction, and a match only ever means "check this exactly".

Script sets are selected by sorted order and fixed arithmetic, never by which
scripts produced favourable results. The mixtures are constructed and are not
wallet-population averages.

### Timing

Building all 1,152 filters takes 0.021 s; the Bloom equivalent takes 1.036 s.
These are benchmark-host numbers on an unloaded machine and say nothing about
phone battery or wallet latency.

## Limitations

- **Trusted indexer, unchanged.** Matching a filter against the wallet's
  accepted block hash binds where the filter claims to be, not what it contains.
  Advancing coverage on a negative result is sound only under the trust policy
  in `docs/transparent_pir_contract.md`. No BIP 157 peer verification, no
  service signalling, and a filter header chain is not a proof of completeness.
- **Coverage begins at Ironwood activation** (3,428,143). A wallet with an
  earlier birthday is not served by this deployment.
- **Loopback only.** No public route exists and no wallet client is enabled.
- **Ingest throughput was measured over a tunnel, not on the host.** Each RPC
  round trip cost ~0.94 s from a laptop, against ~0 ms on the coordinator. The
  full 1,152-block range took 1,045 s under those conditions; on the host the
  RPC is not the limiting factor. Batching previous-output lookups 16 at a time
  cut 200 blocks from ~480 s to 120 s and leaves filter bytes unchanged.
- **The range request reveals the interval being synchronized, and when.** This
  profile does not hide that.
- `bitcoin`'s `std` feature pulls in `secp256k1`, which this crate never uses.
  That weight matters for a mobile build and has not been addressed.
