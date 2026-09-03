# Repository guidance

The root workspace is the active Enhance PIR product. Keep protocol-facing
types and the client in `pir/enhance`; keep ingestion, persistence, HTTP, and
worker orchestration in `server/enhance-pir-server`.

Run `make check` before submitting changes. Release mode is required for the
full-shard cryptographic tests.

`demos/legacy-spendability` contains inactive nullifier and witness demos. It
is excluded from the root workspace and CI. Do not introduce dependencies from
active crates to it. Check it manually with `make demo-check` only when editing
the archived demos.

`docs/archive` is historical context and does not specify current behavior.
Operational files belong under `ops/`. The production workflow performs a
coordinated rollout and must preserve the one-time legacy service rollback path.
