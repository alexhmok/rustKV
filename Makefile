# Task runner for rustkv. GNU Make (ships with macOS; `just` is not assumed installed).

.PHONY: build test fmt lint run clean

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

# Phase 7 will add: `cluster` (3 local processes) and Docker Compose targets.

clean:
	cargo clean
