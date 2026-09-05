# Transparent filter range envelope, version 1

Status: specification of the implemented format. Versioned separately from
BIP 158: the envelope may change without any filter byte changing, and a filter
byte may never change because the envelope did.

The reference implementation is `pir/transparent-filter/src/envelope.rs`, and
`pir/transparent-filter/tests/golden_envelope.rs` pins the bytes below.

## Conventions

- Integers are unsigned, fixed width, little-endian.
- `CompactSize` is the Bitcoin variable-length integer, and must be canonical:
  the shortest encoding for the value. A noncanonical encoding is rejected.
- **Block hashes are serialized in internal (little-endian) byte order.**
  Display hex — the reversed form shown by RPC and explorers — never appears in
  binary serialization. JSON metadata endpoints use display hex, and those are
  the only place it appears.

## Batch

| Field | Type | Notes |
| --- | --- | --- |
| magic | 4 bytes | `ZTFB` (`5a 54 46 42`) |
| version | u16 | 1 |
| genesis | 32 bytes | Chain identity, internal order |
| profile length | CompactSize | at most 64 |
| profile | bytes | UTF-8, e.g. `zcash-transparent-basic-v1` |
| start height | u64 | first record's height |
| stop block hash | 32 bytes | terminal block of the overall range, internal order |
| record count | CompactSize | at most 1,000 |
| records | | `record count` records, ascending, contiguous |

## Record

| Field | Type | Notes |
| --- | --- | --- |
| height | u64 | |
| block hash | 32 bytes | internal order |
| filter length | CompactSize | |
| filter | bytes | raw BIP 158 filter, copied verbatim |

## Rules

- Records are contiguous and ascending: the record at index `i` is at height
  `start height + i`. Gaps, duplicates and reordering are therefore all a single
  check.
- A batch carries at most 1,000 records. A longer range is several batches, and
  splitting a range differently does not change any filter byte.
- Trailing bytes after the last record are an error.
- A claimed record count that cannot fit in the remaining bytes is rejected
  before any allocation sized by that count.

## What the receiver must still check

Decoding proves only that the bytes are a well formed batch. Separately, and
against wallet-owned state rather than anything in the batch:

- the genesis hash and profile are the ones requested;
- the start height and terminal block hash are the ones requested;
- the record count is exactly what the requested range implies;
- every record's height and hash match the wallet's **own** accepted chain;
- every filter passes full BIP 158 validation before any match result is used.

`check_batch` in `pir/transparent-filter/src/client.rs` performs these.

## What a request may contain

A range request carries the chain identity, the profile, a start height and a
terminal block hash. It carries no address, script, outpoint, match, or any
partition derived from them.

The range itself reveals which interval the requester is synchronizing, and
when. This profile does not hide that timing and coverage information.
