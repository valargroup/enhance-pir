# Legacy spendability demos

This independent Cargo workspace preserves the original nullifier PIR, witness
PIR, combined server, shared ingest code, and protobuf definitions. It is not
part of the active Enhance product, root workspace, CI, or deployment.

Build it manually from the repository root:

```sh
cargo check --manifest-path demos/legacy-spendability/Cargo.toml --workspace
```
