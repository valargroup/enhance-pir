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

## Historical Prototypes

- DAG Sync POC:
   * https://github.com/zakura-core/wallet-libraries/pull/14
   * https://github.com/chainapsis/vizor-wallet/tree/roman/ironwood-memo-pir-dag-archive

- Memo PIR
   * https://github.com/zakura-core/wallet-libraries/pull/13
   * https://github.com/chainapsis/vizor-wallet/pull/601
