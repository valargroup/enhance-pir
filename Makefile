.PHONY: build check test run-server run-worker demo-check fmt

build:
	cargo build --release --workspace --bins

check:
	cargo fmt --all --check
	cargo clippy --workspace --all-targets --all-features -- -D warnings
	cargo test --workspace --release

test:
	cargo test --workspace --release

run-server:
	cargo run --release -p enhance-pir-server --bin enhance-pir-server -- --help

run-worker:
	cargo run --release -p enhance-pir-server --bin enhance-pir-worker -- --help

demo-check:
	cargo check --manifest-path demos/legacy-spendability/Cargo.toml --workspace

fmt:
	cargo fmt --all
	cargo fmt --manifest-path demos/legacy-spendability/Cargo.toml --all
