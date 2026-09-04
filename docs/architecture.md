# Enhance PIR architecture

Enhance PIR is split at the network boundary.

- `pir/enhance` defines the wire metadata, record layout, query preparation,
  response decoding, and reference CLI. It has no server or chain-ingest code.
- `server/enhance-pir-server` ingests canonical Ironwood outputs, stores the
  append-only record journal, seals PIR shards, coordinates workers, and serves
  the Enhance v1 HTTP API.
- `server/pir-apm` observes the running fleet.

Every logical output position maps to a 724-byte `EnhanceRecord`. Nine records
form one 6,516-byte row. This is the maximum that fits in two PIR instances;
ten records would require a third instance. A record contains only `ephemeralKey`,
`encCiphertext`, `cv_net`, and `outCiphertext`; transaction IDs, nullifiers,
commitments, heights, and witness data are not stored in the active table.

The protocol identifier is `ironwood-enhance-pir-v1` and schema version is 5.
Old memo/action endpoints and storage are not accepted as aliases. This is a
breaking migration so that clients cannot accidentally mix incompatible record
layouts.

## Worker topology

The coordinator assigns consecutive ranges of six shards to stable logical
worker groups. Each group has two active-active replicas holding identical
rows, CRS material, and retained generations. Different groups evaluate in
parallel; within a group, one ready replica evaluates a query and its peer is
used for load balancing or retry. Only one partial per group is included in the
combined answer.

A generation is published when at least one replica in every used group has
prepared and activated its complete assignment. Replica readiness is tracked
per generation, so a recovering replica is not selected for generations it
does not hold. Group order is append-only because it fixes shard ownership;
replicas inside a group may be replaced without moving shards.
