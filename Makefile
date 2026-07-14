# Task runner for rustkv. GNU Make (ships with macOS; `just` is not assumed installed).

.PHONY: build test fmt lint run cluster compose-up compose-down partition-test clean

build:
	cargo build

test:
	cargo test

fmt:
	cargo fmt

# Must pass at every checkpoint.
lint:
	cargo fmt --check
	cargo clippy --all-targets -- -D warnings

# Single-node server. Override the listen address with:
#   RUSTKV_LISTEN=127.0.0.1:9000 make run
run:
	cargo run

# Local 3-node cluster (three processes; client APIs on 8081-8083).
cluster: build
	./scripts/run-cluster.sh

# 3-node cluster in Docker Compose (requires the Docker daemon running).
# Client APIs on localhost:8081-8083; see README for partition testing.
compose-up:
	docker compose up --build -d

compose-down:
	docker compose down

# Scripted end-to-end partition test against the compose cluster (phase 17).
# Requires the Docker daemon; wipes the compose volumes; refuses to run if
# rustkv containers are already up. Not part of `cargo test`.
partition-test:
	./scripts/partition-test.sh

clean:
	cargo clean

# --- CI targets (testing-regime phase T1) -------------------------------

.PHONY: ci soak

# Local parity with .github/workflows/ci.yml: lint + debug compile check +
# locked release tests (release because the cluster_http flake class is
# CPU-starvation-driven and only reproduces in debug).
ci:
	cargo fmt --check
	cargo clippy --all-targets --locked -- -D warnings
	cargo build --locked
	cargo test --release --locked

# Extended #[ignore]d soaks (tests/faults.rs, tests/jepsen.rs) in release
# mode. Seed count via RUSTKV_SOAK_SEEDS (the tests' own default is 24;
# 256 here for CI/nightly — ~90s per suite locally at 256).
RUSTKV_SOAK_SEEDS ?= 256
soak:
	RUSTKV_SOAK_SEEDS=$(RUSTKV_SOAK_SEEDS) cargo test --release --locked \
		--test faults --test jepsen -- --ignored extended_soak
