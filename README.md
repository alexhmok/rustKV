# rustkv

A distributed key-value store, similar in spirit to etcd. Multiple nodes will form a
cluster kept consistent by a hand-implemented Raft consensus core (no consensus or RPC
crates). The system is CP: under a network partition, the minority side refuses writes
rather than diverging.

**Current status: phase 0** — a single-node, in-memory store behind the HTTP API. No
consensus or persistence yet. See [PLAN.md](PLAN.md) for the roadmap and progress.

## Prerequisites

Just [rustup](https://rustup.rs/). `rust-toolchain.toml` pins the compiler (1.89.0) and
the clippy/rustfmt components; rustup installs them automatically on first build.
`Cargo.lock` is committed, so dependency versions are exact.

Docker is **not** required for phases 0–6. Phase 7 adds a Docker Compose cluster for
partition testing, which also becomes the zero-host-setup onboarding path.

## Build / run / test

Common tasks are in the `Makefile` (GNU Make, preinstalled on macOS):

```sh
make build   # cargo build
make test    # cargo test (unit + HTTP integration tests)
make lint    # cargo fmt --check && cargo clippy --all-targets -- -D warnings
make run     # run the single-node server on 127.0.0.1:8080
```

The listen address can be overridden with an argument or env var:

```sh
cargo run -- 127.0.0.1:9000
RUSTKV_LISTEN=127.0.0.1:9000 make run
```

Logging uses `tracing`; control verbosity with `RUST_LOG` (default `info`), e.g.
`RUST_LOG=debug make run`.

## HTTP API

| Method   | Path     | Behavior                                                          |
| -------- | -------- | ----------------------------------------------------------------- |
| `GET`    | `/{key}` | `200` with the stored JSON, `404` if absent                        |
| `PUT`    | `/{key}` | `201` on write (create or overwrite), `400` if body is not valid JSON |
| `DELETE` | `/{key}` | `204` (idempotent — also `204` if the key was absent)              |

The `Content-Type` header is not required; the body just has to be valid JSON.

```sh
curl -i -X PUT localhost:8080/greeting -d '{"hello": "world"}'   # 201
curl -i localhost:8080/greeting                                  # 200 {"hello":"world"}
curl -i -X DELETE localhost:8080/greeting                        # 204
curl -i localhost:8080/greeting                                  # 404
```

Once Raft lands (phase 5+), writes commit to a majority of the cluster before the
response is sent, and non-leaders forward/redirect writes to the leader.

## Layout

- `src/lib.rs` — the library: KV state machine (`store`), client HTTP API (`api`);
  later phases add the Raft core, persistence, and the transport trait with real
  (HTTP) and simulated (deterministic, seeded) implementations.
- `src/main.rs` — thin binary: env config, tracing setup, axum server.
- `tests/` — integration tests against a real server on an ephemeral port.
- `CLAUDE.md` — durable engineering rules for this repo (dependency whitelist,
  architecture constraints, standards).
