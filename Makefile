# PIR Server
# Top-level Makefile for local development
#
# Single binary: spend-server (combined nullifier + witness PIR)
# Feature flags: --features nullifier, --features witness, or both (default)
#
# Usage: make build && make run

# ── Configuration ────────────────────────────────────────────────────
DATA_DIR  ?= ./data
LISTEN    ?= 0.0.0.0:8080
LWD_URL   := https://us.zec.stardust.rest:443

# ── Targets ──────────────────────────────────────────────────────────

.PHONY: build build-nullifier build-witness build-ipir \
        run run-nullifier run-witness run-ipir \
        test test-fast test-slow test-ipir test-bench \
        check fmt lint clean help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

build: ## Build spend-server with both nullifier + witness (default)
	cargo build --release -p combined-server --features ypir

build-nullifier: ## Build spend-server with nullifier only
	cargo build --release -p combined-server --no-default-features --features "nullifier,ypir"

build-witness: ## Build spend-server with witness only
	cargo build --release -p combined-server --no-default-features --features "witness,ypir"

build-ipir: ## Build spend-server with IPIR+SP — what production ships
	cargo build --release -p combined-server --features ipir

run: ## Run spend-server with both nullifier + witness (default)
	cargo run --release -p combined-server --features ypir -- \
		--lwd-url $(LWD_URL) \
		--data-dir $(DATA_DIR) \
		--listen $(LISTEN)

run-nullifier: ## Run spend-server with nullifier only
	cargo run --release -p combined-server --no-default-features --features "nullifier,ypir" -- \
		--lwd-url $(LWD_URL) \
		--data-dir $(DATA_DIR) \
		--listen $(LISTEN)

run-witness: ## Run spend-server with witness only
	cargo run --release -p combined-server --no-default-features --features "witness,ypir" -- \
		--lwd-url $(LWD_URL) \
		--data-dir $(DATA_DIR) \
		--listen $(LISTEN)

run-ipir: ## Run spend-server with IPIR+SP — what production ships
	cargo run --release -p combined-server --features ipir -- \
		--lwd-url $(LWD_URL) \
		--data-dir $(DATA_DIR) \
		--listen $(LISTEN)

# ── Tests ────────────────────────────────────────────────────────────
# Three tiers; see CLAUDE.md for the policy.
#   fast  - hermetic: no network, no PIR crypto. The default; run this.
#   slow  - mainnet ingest + PIR round-trips. CI and deliberate manual runs.
#   bench - throughput and scaling benchmarks. Manual only, needs GBs of RAM.
#
# The slow and bench tests self-skip unless PIR_SLOW_TESTS / PIR_BENCH is set,
# so nothing below the slow tier can accidentally reach the network.

test: test-fast ## Alias for test-fast

test-fast: ## Hermetic tests: no network, no PIR crypto. Safe to run anytime.
	cargo test --workspace

test-slow: ## Mainnet + PIR round-trips (minutes, needs network access)
	PIR_SLOW_TESTS=1 cargo test --workspace --all-features --release

test-ipir: ## Run the IPIR integration tests (release mode)
	cargo test -p spend-server -p witness-server -p spend-client \
		--features ipir --release --test ipir_test --test end_to_end

test-bench: ## Throughput + scaling benchmarks (manual only; needs GBs of RAM)
	PIR_SLOW_TESTS=1 PIR_BENCH=1 cargo test --release \
		-p combined-server --features ypir --test throughput_3tps -- --nocapture
	PIR_BENCH=1 cargo test --release \
		-p spend-server --features ypir --test ypir_bench -- --nocapture

check: ## Type-check everything without codegen (fastest feedback)
	cargo check --workspace --all-targets

fmt: ## Format the workspace
	cargo fmt --all

lint: ## Clippy over all targets and features
	cargo clippy --workspace --all-targets --all-features

clean: ## Remove build artifacts and data files
	# Warning: discards several GB under target/ and forces a full rebuild,
	# including the git PIR dependencies. Rarely what you want.
	cargo clean
	rm -rf $(DATA_DIR)
