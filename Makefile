.PHONY: build check test run-server run-worker load-test demo-check fmt

LOAD_TEST_DURATION ?= 60s
LOAD_TEST_PARALLELISM ?= 8
LOAD_TEST_WARMUP ?= 10s
LOAD_TEST_JSON ?= load-test-summary.json
LOAD_TEST_MAX_ERROR_RATE ?= 0.01

build:
	cargo build --release --workspace --bins --features enhance-pir/cli

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

load-test:
	@if [ -z "$(LOAD_TEST_URL)" ]; then \
		echo "LOAD_TEST_URL is required (for example, https://enhance-pir.valargroup.dev)" >&2; \
		exit 2; \
	fi
	cargo run --release -p enhance-pir-load-test -- \
		--server "$(LOAD_TEST_URL)" \
		--duration "$(LOAD_TEST_DURATION)" \
		--parallelism "$(LOAD_TEST_PARALLELISM)" \
		--warmup "$(LOAD_TEST_WARMUP)" \
		$(if $(strip $(LOAD_TEST_JSON)),--json-out "$(LOAD_TEST_JSON)") \
		$(if $(strip $(LOAD_TEST_SEED)),--seed "$(LOAD_TEST_SEED)") \
		$(if $(strip $(LOAD_TEST_SLO_P99_MS)),--slo-p99-ms "$(LOAD_TEST_SLO_P99_MS)") \
		--max-error-rate "$(LOAD_TEST_MAX_ERROR_RATE)"

demo-check:
	cargo check --manifest-path demos/legacy-spendability/Cargo.toml --workspace

fmt:
	cargo fmt --all
	cargo fmt --manifest-path demos/legacy-spendability/Cargo.toml --all
