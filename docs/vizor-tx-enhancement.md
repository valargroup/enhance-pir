# Vizor Ironwood transaction enhancement

Enhance PIR replaces transaction-specific retrieval for the encrypted output
data a wallet needs after compact scanning. The wallet derives an Ironwood
output position, sends a private real-or-dummy query, and receives one fixed
record without revealing the selected position.

## Record format

Schema 6 stores exactly 725 bytes per output position:

| Offset | Length | Field | Use |
| ---: | ---: | --- | --- |
| 0 | 32 | `ephemeralKey` | Note key agreement |
| 32 | 580 | `encCiphertext` | Note and authenticated memo |
| 612 | 32 | `cv_net` | OVK-based outgoing recovery |
| 644 | 80 | `outCiphertext` | OVK-based outgoing recovery |
| 724 | 1 | `flags` | Bit 0: containing transaction has transparent inputs or outputs |

Bits 1 through 7 are reserved and must be zero. Nine consecutive records form a
6,525-byte PIR row. The client privately
retrieves the row and selects the requested record locally. The active table
does not contain txids, nullifiers, note commitments, heights, or witness data.

## Protocol boundary

`pir/enhance` owns public record and generation types plus client query logic.
`server/enhance-pir-server` owns canonical ingestion, the append-only journal,
sealed shards, workers, and HTTP routing. The protocol identifier is
`ironwood-enhance-pir-v1`; clients must reject another identifier, schema,
record width, row width, or setup seed.

The flags byte is transaction metadata supplied by the snapshot builder. It is
not authenticated by note decryption or committed by the on-chain note
commitment. Wallet clients reject reserved bits and expose bit 0 to the
application; persistence and transaction-ID fallback policy are intentionally
deferred. Applications that act on bit 0 trust the builder to report it
correctly.

The client uses only:

- `GET /v1/health`
- `GET /v1/enhance/init`
- `POST /v1/enhance/query`

This is a breaking replacement for the former memo/action API. There are no
compatibility aliases because interpreting an old record with the new offsets
would be unsafe. Production rollout remains blocked until Vizor has migrated.
