# rustkv

A distributed key-value store, similar in spirit to etcd. Multiple nodes will form a
cluster kept consistent by a hand-implemented Raft consensus core (no consensus or RPC
crates). The system is CP: under a network partition, the minority side refuses writes
rather than diverging.

**Current status: phase 5** — client writes go through the replicated log: a PUT/DELETE
is only acknowledged after the entry is committed by a majority and applied. Non-leaders
redirect writes to the leader. The binary runs a single-node cluster (fully persistent —
state is rebuilt from the log on restart); multi-node clusters currently exist on the
in-process simulated transport (tested end-to-end over real HTTP client APIs), and the
node-to-node HTTP transport arrives in phase 7. See [PLAN.md](PLAN.md).

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

The listen address and data directory can be overridden:

```sh
cargo run -- 127.0.0.1:9000
RUSTKV_LISTEN=127.0.0.1:9000 RUSTKV_DATA_DIR=/tmp/rustkv make run
```

The data directory (default `./rustkv-data`) holds the Raft log and hard state; the KV
state is reconstructed from it on startup.

Logging uses `tracing`; control verbosity with `RUST_LOG` (default `info`), e.g.
`RUST_LOG=debug make run`.

## HTTP API

| Method   | Path     | Behavior                                                          |
| -------- | -------- | ----------------------------------------------------------------- |
| `GET`    | `/{key}` | `200` with the stored JSON, `404` if absent                        |
| `PUT`    | `/{key}` | `201` once committed by a majority, `400` if body is not valid JSON |
| `DELETE` | `/{key}` | `204` once committed (idempotent — also `204` if the key was absent) |

The `Content-Type` header is not required; the body just has to be valid JSON.

```sh
curl -i -X PUT localhost:8080/greeting -d '{"hello": "world"}'   # 201
curl -i localhost:8080/greeting                                  # 200 {"hello":"world"}
curl -i -X DELETE localhost:8080/greeting                        # 204
curl -i localhost:8080/greeting                                  # 404
```

Cluster semantics (CP):

- Writes are acknowledged only after majority commit + local apply. A node cut off
  from the majority answers `504` — the outcome is unknown and the write is never
  acknowledged (it is truncated away unless it later commits).
- A non-leader answers writes with `307 Temporary Redirect` to the leader's client URL
  when known, else `503` (safe to retry). `503` is also used when leadership changed
  mid-write (definitely not applied, safe to retry).
- Reads are served locally and may be stale on followers or a just-deposed leader;
  linearizable reads (ReadIndex/leases) are out of scope.

## Layout

- `src/lib.rs` — the library: KV state machine (`store`), client HTTP API (`api`);
  later phases add the Raft core, persistence, and the transport trait with real
  (HTTP) and simulated (deterministic, seeded) implementations.
- `src/main.rs` — thin binary: env config, tracing setup, axum server.
- `tests/` — integration tests against a real server on an ephemeral port.
- `CLAUDE.md` — durable engineering rules for this repo (dependency whitelist,
  architecture constraints, standards).
