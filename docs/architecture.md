# Enhance PIR architecture

Enhance PIR is split at the network boundary.

- `pir/enhance` defines the wire metadata, record layout, query preparation,
  response decoding, and reference CLI. It has no server or chain-ingest code.
- `pir/transparent-spend` defines the outpoint-keyed two-tier spend lookup.
- `server/enhance-pir-server` ingests canonical Ironwood outputs, stores the
  append-only record journal, seals PIR shards, coordinates workers, and serves
  the Enhance v1 HTTP API.
- `server/pir-apm` observes the running fleet.

Every logical output position maps to a 725-byte `EnhanceRecord`. Nine records
form one 6,525-byte row. This is the maximum that fits in two PIR instances;
ten records would require a third instance. A record contains `ephemeralKey`,
`encCiphertext`, `cv_net`, `outCiphertext`, and a transaction-shape flag byte.
Bit 0 means transparent inputs are present and bit 1 means transparent outputs
are present; clients reject all reserved bits.

The protocol identifier is `ironwood-enhance-pir-v1` and schema version is 6.
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

The transparent-spend cold and warm tables share one separate worker. Cold
covers genesis through `tip - 100000`; warm covers the remaining best-chain
blocks through tip. Both tiers and Enhance are published under the same tip
height, block hash, and Ironwood tree size. A wallet always queries both tiers.
