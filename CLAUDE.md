# spendability-pir — working notes for agents

Rust workspace, 16 crates. The product is the `memo/` PIR fleet (coordinator,
workers, ingest; deployed by `.github/workflows/deploy-pir-fleet.yml`) and
`deploy/pir-apm` (its monitoring sidecar; hermetic unit tests only). The
`nullifier/` and `witness/` subsystems and the `combined-server/` that shipped
them are **legacy**: no longer deployed, kept as libraries (`memo-pir` uses
`commitment-tree-db`) until they are removed. See `docs/pir_deployment_architecture.md`
for the scope.
Toolchain is pinned to 1.91.0 by `rust-toolchain.toml`. `protoc` must be on
PATH — `shared/chain-ingest` has a `tonic-build` build script.

## Tests: run `make test-fast`

```sh
make test-fast     # or: cargo test --workspace / cargo ft
```

Hermetic: no network, no PIR crypto setup. **~12s warm, ~70s from cold.** It is
safe to run at any time and is the only test command you should reach for by
default.

If it takes materially longer than that, something is wrong — stop and look,
don't wait it out.

### Do not run these unless explicitly asked

| Command | Cost | Why |
|---|---|---|
| `make test-slow` | many minutes, needs internet | Mainnet ingest + full PIR round-trips |
| `make test-bench` | minutes, multiple GB of RAM | Throughput and scaling benchmarks |
| `cargo test --workspace --all-features` | minutes | Builds and runs the YPIR/IPIR backends |

Never set `PIR_SLOW_TESTS` or `PIR_BENCH` yourself. They exist so the slow tests
stay opt-in; setting them is how you reintroduce the hang.

## Why the tiers exist

`cargo test --workspace` used to hit **mainnet lightwalletd** through
`witness/commitment-tree-db/tests/tree_correctness.rs`, which had no `#[ignore]`
and no gate. It pulled a full 65,536-leaf shard in 10,000-block batches and took
minutes — or hung indefinitely on a slow endpoint. That is what the gates fix.

Two gates, defined in `shared/pir-types/src/lib.rs`:

- `pir_types::skip_unless_slow!()` — keyed on `PIR_SLOW_TESTS`. For anything
  that needs the network or takes minutes.
- `pir_types::skip_unless_bench!()` — keyed on `PIR_BENCH`. For benchmarks:
  gigabytes of RAM, timings rather than pass/fail signal, never run in CI.

Put the macro on the **first line** of the test body. A skipped test prints a
`SKIP` line so a skipped run is visibly different from a passing one.

### What is gated, and why it is slow

| Test | Gate | Cost |
|---|---|---|
| `commitment-tree-db/tests/tree_correctness.rs` (2) | slow | Mainnet ingest of a whole shard + 16 levels of Sinsemilla |
| `witness-server/tests/pir_round_trip.rs` | slow | Two full mainnet syncs + real YPIR DB + encrypted HTTP |
| `witness-client/tests/e2e_mainnet.rs` (2) | slow + `#[ignore]` | Mainnet ingest in 10k-block batches |
| `combined-server/tests/throughput_3tps.rs::rebuild_under_20s_at_3tps` | slow | Two PIR builds, timed |
| `combined-server/tests/throughput_3tps.rs::sustained_5tps_15s_blocks` | slow | Hard-coded 120 s wall-clock run |
| `combined-server/tests/throughput_3tps.rs::bench_scaling_ceiling` | bench | DB geometries up to 1.8 GB |
| `spend-server/tests/ypir_bench.rs` | bench | 56 MB DB, timing loop |

The YPIR/IPIR round-trip tests (`spend-server/tests/{ypir,ipir}_test.rs`,
`witness-server/tests/ipir_test.rs`, `spend-client/tests/end_to_end.rs`) need no
env gate: they sit behind `#![cfg(feature = "ypir")]` / `"ipir"` and compile to
nothing on the default path. **The rule is that the fast tier never passes
`--features ypir` or `--features ipir`.**

Adding a test that reaches the network or takes more than a second or two?
Gate it, and add a row above.

## Features

| Feature | Crates | Meaning |
|---|---|---|
| `ypir` | servers | Compiles the YPIR backend |
| `ipir` | servers, clients | Compiles the IPIR+SP backend for the legacy servers; `memo-pir` always builds with iPIR+SP |
| `nullifier` / `witness` | `combined-server` | Which subsystem to include; both on by default |
| `live` | `spend-client` | Manual tests against a running server (`PIR_SERVER_URL`) |

Backend selection in the **client** crates is `cfg(not(feature = "ipir"))`:
with `ipir` off, YPIR is the active backend. So `ypir` is a hard dependency of
`spend-client` / `witness-client` and cannot be made optional — the `ypir`
feature there only pulls the matching server backend in for tests.

`combined-server` builds a binary named **`spend-server`**, which collides with
the `spend-server` crate's own binary. Cargo warns about the filename clash on
every workspace build; it is pre-existing and harmless.

## Other commands

```sh
make check    # cargo check --workspace --all-targets — fastest feedback
make fmt      # cargo fmt --all
make lint     # clippy, all targets and features (matches CI)
```

Aliases `cargo ft` / `cargo ck` / `cargo lint` are in `.cargo/config.toml`.

Avoid `make clean` / `cargo clean`: it discards several GB under `target/` and
forces a full rebuild of the git PIR dependencies.

## CI

`.github/workflows/ci.yml`. PRs run lint plus the fast per-subsystem jobs. The
slow jobs (`test-tree-correctness`, `test-pir-round-trip`, `test-ypir`) are
push-to-main only and set `PIR_SLOW_TESTS=1` explicitly — without that env var
they would skip and go green on nothing.
