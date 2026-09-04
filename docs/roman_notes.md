# Roman's notes

## Enhance PIR Value Structure

- ephemeralKey — note key agreement.
- encCiphertext — note and authenticated memo.
- cv_net and outCiphertext — OVK-based outgoing recovery.

### encCiphertext

encCiphertext is 580 bytes total:
```
Encrypted plaintext:
  0..1       version                 1 byte (0x03 for Ironwood)
  1..12      diversifier           11 bytes
  12..20     value                  8 bytes
  20..52     rseed                 32 bytes
  52..564    memo                 512 bytes

Authentication:
  564..580   Poly1305 tag           16 bytes
                                      ─────
                                      580 bytes
```

The first 52 encrypted bytes contain everything necessary to discover and reconstruct the note:
```
version + diversifier + value + rseed = 52 bytes
```

Given ephemeralKey and an IVK, the wallet derives the note-encryption key and decrypts that prefix. It then validates the candidate using cmx and the derived ephemeral key.

Compact scanning does not need the memo so it is ommitted for bandwidth purposes.

### ourCiphertext

outCiphertext is separate:
```
Encrypted outgoing plaintext:
  pk_d                  32 bytes
  esk                   32 bytes
Authentication tag      16 bytes
                        ─────
                         80 bytes
```

So the encrypted-output material is:
```
ephemeralKey[32]
encCiphertext[580]
outCiphertext[80]
```

`cv_net`, `cmx`, and the OVK derive the key that opens `outCiphertext`. Its recovered pk_d + esk then derive the key that opens encCiphertext.

## Incoming Decryption

```
ivk + ephemeralKey
        ↓ Diffie–Hellman
shared secret
        ↓ KDF
note encryption key
        ↓
decrypt encCiphertext
        ↓
note + value + recipient + memo
```

Compact blocks provide only the first 52 bytes of `encCiphertext`. That discovers the note, but the remaining 528 bytes are needed for the memo and authentication tag.

## Outgoing Recovery

The sender cannot use an IVK to decrypt outputs sent to someone else. Instead it uses its outgoing viewing key (ovk):
```
ovk + cv_net + cmx + ephemeralKey
                ↓
       outgoing cipher key (ock)
                ↓
       decrypt outCiphertext
                ↓
       recipient pk_d + ephemeral secret esk
                ↓
       esk + pk_d → shared secret
                ↓
       decrypt encCiphertext
                ↓
       recipient + value + memo
```

## PIR Layout

Key: action's output note's index in the commitment tree
Value (725 bytes):
```
Offset   Size   Field
0        32     ephemeralKey
32       580    encCiphertext
612      32     cv_net
644      80     outCiphertext
724      1      flags (bit 0 = transaction has transparent inputs or outputs)
                ───
                725 bytes
```

With nine records per row:
```
9 × 725 = 6,525-byte PIR row
row  = position / 9
slot = position % 9
```

Incoming recovery uses `ephemeralKey` + `encCiphertext`. Outgoing recovery additionally uses `cv_net` + `outCiphertext`, with `cmx` supplied by the compact action.

Why 9 records per row?
- Two 2048 × 14-bit PIR instances carry 7,168 bytes. Nine records use 6,525 bytes; ten would need 7,250 bytes and a third instance.
- Nine is therefore the densest layout with the same cryptographic instance count and response size as eight.
- At 136,425 positions it uses 15,159 rows, which rounds to 16,384 logical rows. Eight records would use 17,054 rows and round to 32,768.
- The division and remainder by constant nine are negligible compared with the PIR work.

## Scalability

1. One controller and multipler workers where each worker has a separate set of shards
2. Because online evaluation is memory-bandwidth-bound, scale horizontally
across workers with independent memory bandwidth. Each worker owns a
disjoint shard range, and the coordinator evaluates all ranges in parallel
A starting configuration is 4 vCPU / 4 GiB workers, sized to keep no more
than approximately 2 GiB of total resident PIR state, including
preprocessing artifacts and retained frontier generations. Confirm the
worker size and shards-per-worker setting with an end-to-end load test
3. Workers are organized into shard groups with two active-active replicas.
Each query uses one ready replica per group and fails over to its peer; the
replica partials are alternatives and must not both be added.
4. Query size grows as the database grows. For example, at 1 million
positions in Ironwood, we are at 0.79 MB query upload. As a result,
we could have a coordinator-workers topology for the most recent 1M
and split the older per 10M. Anonymity set is still large and is a strict improvement over today.

Controller fans-out

## Historical Prototypes

- DAG Sync POC:
   * https://github.com/zakura-core/wallet-libraries/pull/14
   * https://github.com/chainapsis/vizor-wallet/tree/roman/ironwood-memo-pir-dag-archive

- Enhance PIR
   * https://github.com/zakura-core/wallet-libraries/pull/13
   * https://github.com/chainapsis/vizor-wallet/pull/601
