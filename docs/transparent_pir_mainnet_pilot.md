# Transparent PIR: first mainnet measurement

Date: 2026-09-04. This is a bounded public-chain sample, with Vizor as the first
intended wallet integration. It measures index inputs, reconstructed transparent
compact messages, and experimental Bloom filters. It does **not** measure a
working directory/history PIR service or mobile wallet sync.

## Data and access

The Zakura repository's `deploy/deployer/README.md` and
`.github/workflows/zakura-mainnet-deploy.yml` document the current fleet.
SSH to `root@104.131.174.28` reached `archive-vct-off`; SSH to
`root@159.65.183.89` reached `us-east-0`. Both services were active.
The archive reported Mainnet, `pruned=false`, and tip 3,472,064 at initial inspection.
Historical data was read from its local JSON-RPC at `127.0.0.1:8232`.

The preserved sample contains blocks **3,470,396–3,470,523 inclusive**, 128 blocks
spanning 9,610 seconds (2 h 40 min between first and last block timestamps).
Its final anchor, rechecked through archive `getblockhash` after collection, is:

```text
00000000000c3d327246f363ae3eb1e51531ad1727d57fc4e0798928fe35e11c
```

Collection initially requested 1,024 blocks ending at 3,471,419. At the first
128-block progress point it had already made 1,898 RPC calls, including resolution
of spent previous outputs. The experiment was reduced to that complete prefix to
bound archive work. The collector was stopped and the prefix's actual anchor was
verified separately. This is not a completed 1,024-block collection. Calls were
serial and capped at 10 starts/second; no node configuration or service changed.

The [compressed public sample](transparent-pir-evaluation/mainnet-3470396-3470523.jsonl.gz)
contains scripts and resolved previous outputs, but no wallet key material or
real wallet groupings. The uncompressed JSONL SHA-256 is:

```text
fd4c5a21200a8dfa4502b71a09849fcf4a1232e9db916b647773716b2e10843a
```

## Observed chain workload

| Metric | Result |
|---|---:|
| Supported transparent receives | 2,558 |
| Supported transparent spends, all previous scripts resolved | 3,387 |
| Distinct P2PKH/P2SH scripts with activity | 1,485 |
| Supported coinbase receives, included above | 254 |
| Outputs both created and spent within this sample | 1,294 |
| Unsupported-script receives / spends | 3 / 0 |
| Sample events per active script: p50 / p90 / p95 / p99 / max | 2 / 4 / 7 / 59 / 678 |

These are **events within the sample**, not lifetime history sizes or wallet
percentiles. In particular, 1,294 outputs disappeared from the UTXO set during
this short window: current-UTXO discovery would omit their completed histories.
Filters include both newly received scripts and scripts of consumed previous
outputs. The three unsupported outputs are outside the contract's script scope;
the compact baseline still carries them as a general transparent stream would.

## Measured payloads

Reconstructed messages use the wallet library's `CompactBlock`, `CompactTx`,
`CompactTxIn`, and `TxOut` protobuf definitions. Txids/hashes are reversed from
RPC display order into protocol byte order. Coinbase null inputs are omitted.
Fees, shielded data and chain metadata are excluded. Consequently this is a
**transparent-only payload baseline**, not captured gRPC traffic or the marginal
cost of adding transparent data to a shielded stream.

| Encoding | Bytes for all 128 blocks |
|---|---:|
| Protobuf messages | 304,937 |
| Protobuf plus five-byte gRPC message envelopes | 305,577 |
| Each message gzip-compressed separately, plus envelopes | 272,264 |
| Entire framed batch gzip-compressed | 217,767 |

Per-message gzip and whole-batch gzip are different delivery assumptions. Neither
includes HTTP/2/TLS or request bytes. A real Vizor comparison must measure its
negotiated compression and shared shielded/transparent stream costs.

The experimental Bloom filters use standard SHA-256 through Python `hashlib`,
with target false-positive probability controlling bit count and hash count.
Each carries an explicit 88-byte experimental range/hash/parameter envelope.
This is a measurement encoding, not an adopted filter protocol. Below are actual
constructed sizes at target probability `10^-6`, before optional batch gzip:

| Interval size | Filters | Bytes including envelopes |
|---|---:|---:|
| 1 block | 128 | 22,829 |
| 16 blocks | 8 | 8,860 |
| 128 blocks | 1 | 5,426 |

All inserted supported scripts matched their filters. A deterministic set of
100 synthetic P2PKH scripts absent from the sample produced zero matches at
`10^-6`; this small test does not validate that false-positive rate statistically.
At `10^-4`, the one-block filters produced one false-positive script/filter
match. Full results for `10^-4`, `10^-5`, and `10^-6` are in
[mainnet-sample-results.json](transparent-pir-evaluation/mainnet-sample-results.json).

Larger sealed intervals imply more delay unless a recent tail/update mechanism
is provided. The 16- and 128-block figures alone do not satisfy the assessment's
two-block publication-freshness gate. No conclusion about 1,024-block intervals
is possible from this 128-block sample.

## What this changes

For an already-synchronized wallet whose supported scripts have no activity or
false matches in this window, the eight 16-block filters total 8,860 bytes versus
272,264 bytes of reconstructed per-message-compressed compact data: **96.75% less
application payload**. This excludes setup and has the coverage, freshness, and
compression limitations above. It is not a general wallet bandwidth claim.

For an active script, using the previous assessment's assumed two bucket requests
and recorded 268,312–318,464-byte query references gives:

```text
8,860 filter bytes + 2 queries = 545,484–645,788 bytes
```

That is **2.00–2.37×** the compact baseline for this window, even before setup and
history pages. This is arithmetic combining measured sample/filter sizes with
other measured PIR geometries; no transparent directory query was executed.
It shows why a filter win for inactive wallets is insufficient to choose the
architecture. Active-wallet cost, actual dictionary geometry and query coalescing
must be measured before committing to the hybrid.

Archive access is no longer a blocker. Remaining empirical work is the historical
script census, broader activity windows, a directory/page PIR prototype, real
Vizor traces and transport behavior, and physical-device measurements. The current
decision remains HOLD production adoption, with evidence supporting further
measurement rather than an architecture commitment.

## Reproduce

The schema snapshot retains its upstream license header. It was copied from
`wallet-libraries/librustzcash/zcash_client_backend/lightwallet-protocol/walletrpc/compact_formats.proto`
in the checkout at `ec97c5a63206ac35350f749dad77f03950b37796`; the analysis records
the actual schema digest, Python/protobuf/protoc versions, and zlib version.
Install/use `protoc` and run:

```sh
uv run --with protobuf==6.33.5 python tools/transparent_pir_sample.py \
  --sample docs/transparent-pir-evaluation/mainnet-3470396-3470523.jsonl.gz \
  --proto docs/transparent-pir-evaluation/compact_formats.proto \
  --output /tmp/transparent-mainnet-sample-results.json
```

To recollect the same bounded range using only local archive RPC reads:

```sh
ssh -o BatchMode=yes -o ConnectTimeout=10 root@104.131.174.28 \
  'python3 - --anchor 3470523 --blocks 128 --max-rps 10' \
  < tools/transparent_pir_collect.py > /tmp/transparent-mainnet-sample.jsonl
```

A fresh collection has different collection metadata and call counts. Block
contents must match the same anchor; analysis refuses incomplete samples, missing
previous-output resolution, height/hash discontinuities, or anchor mismatches.
