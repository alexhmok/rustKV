# rustkv

A distributed key-value store, similar in spirit to etcd. Multiple nodes will form a
cluster kept consistent by a hand-implemented Raft consensus core (no consensus or RPC
crates). The system is CP: under a network partition, the minority side refuses writes
rather than diverging.

**Current status: phase 7 (feature-complete)** — a full CP cluster: writes go through
the replicated log and are acknowledged only after majority commit; non-leaders
redirect writes to the leader; nodes rebuild their KV state from the persisted log on
restart. Consensus is verified two ways: deterministic seeded fault tests on the
simulated transport (partitions, leader crash/restart mid-write, randomized fault
schedules — asserting at most one leader per term, no confirmed write lost, identical
logs and state machines after recovery), and end-to-end over the real HTTP transport
(in-process, three OS processes, and Docker Compose with real network partitions).
See [PLAN.md](PLAN.md).

## Prerequisites

Just [rustup](https://rustup.rs/). `rust-toolchain.toml` pins the compiler (1.89.0) and
the clippy/rustfmt components; rustup installs them automatically on first build.
`Cargo.lock` is committed, so dependency versions are exact.

Docker is optional: it is only needed for the Compose cluster (partition testing /
zero-host-setup onboarding, see below). Everything else — including the 3-process
local cluster and the whole test suite — needs only rustup.

## Build / run / test

Common tasks are in the `Makefile` (GNU Make, preinstalled on macOS):

```sh
make build        # cargo build
make test         # cargo test (68 tests: unit, sim-cluster, fault, e2e, 3-process)
make lint         # cargo fmt --check && cargo clippy --all-targets -- -D warnings
make run          # single-node server on 127.0.0.1:8080
make cluster      # local 3-node cluster (three processes, APIs on 8081-8083)
make compose-up   # 3-node Docker Compose cluster (requires the Docker daemon)
make compose-down
```

Configuration is env-based (no CLI-parsing crate is on the dependency whitelist);
see `src/config.rs` for all variables:

```sh
RUSTKV_LISTEN=127.0.0.1:9000 RUSTKV_DATA_DIR=/tmp/rustkv make run
```

The data directory (default `./rustkv-data`) holds the Raft log and hard state; the KV
state is reconstructed from it on startup. Multi-node membership is fixed via
`RUSTKV_NODE_ID` / `RUSTKV_RAFT_LISTEN` / `RUSTKV_PEERS` / `RUSTKV_PEER_CLIENT_URLS` —
`scripts/run-cluster.sh` shows a complete example.

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

## Docker Compose cluster and partition testing

With the Docker daemon running:

```sh
make compose-up               # build the image, start nodes on localhost:8081-8083
curl -i -L -X PUT localhost:8081/k -d '{"v":1}'   # -L follows leader redirects
```

The Compose file uses two networks: `rustkv-client` carries the published ports and
`rustkv-raft` carries node-to-node traffic (via `*-raft` DNS aliases that exist only
there). Partitioning a node therefore cuts consensus traffic while its client API
stays reachable — exactly what's needed to watch CP behavior:

```sh
docker network disconnect rustkv-raft rustkv-node1          # partition node1
curl -i --max-time 20 -X PUT localhost:8081/doomed -d '1'   # 504 if node1 led
docker network connect --alias node1-raft rustkv-raft rustkv-node1   # heal
```

While a (former) leader is partitioned it answers writes with `504` and never commits
them; the majority side elects a successor and keeps serving; on heal, the isolated
node's uncommitted entries are truncated away and it converges. Data lives in named
volumes, so `docker restart rustkv-node1` demonstrates log-replay recovery.

## Layout

- `src/lib.rs` — the library: KV state machine (`store`), client HTTP API (`api`);
  later phases add the Raft core, persistence, and the transport trait with real
  (HTTP) and simulated (deterministic, seeded) implementations.
- `src/main.rs` — thin binary: env config, tracing setup, axum server.
- `tests/` — integration tests against a real server on an ephemeral port.
- `CLAUDE.md` — durable engineering rules for this repo (dependency whitelist,
  architecture constraints, standards).
