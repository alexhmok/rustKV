# rustkv

A distributed key-value store, similar in spirit to etcd. Multiple nodes will form a
cluster kept consistent by a hand-implemented Raft consensus core (no consensus or RPC
crates). The system is CP: under a network partition, the minority side refuses writes
rather than diverging.

**Current status: complete (phases 0–17)** — a full CP cluster: writes go through the
replicated log and are acknowledged only after majority commit; non-leaders redirect
writes to the leader; nodes rebuild their KV state from the persisted log (and
snapshot) on restart. Reads are linearizable by default (ReadIndex, phase 9), with
`?stale=true` as an explicit local-read opt-out. Later phases added PreVote,
CheckQuorum, exactly-once client sessions, snapshotting/log compaction with
InstallSnapshot, dynamic single-server membership, and HTTP connection pooling.
Consensus is verified four ways: deterministic seeded fault tests on the simulated
transport (partitions, leader crash/restart mid-write, randomized fault schedules —
asserting at most one leader per term, no confirmed write lost, identical logs and
state machines after recovery); end-to-end over the real HTTP transport (in-process,
three OS processes, and Docker Compose with real network partitions); a
Jepsen-style harness (`tests/jepsen.rs`) — concurrent clients + a partition nemesis
recording timed histories, checked by a Wing&Gong linearizability checker that
confirms default reads linearizable and pinpoints replayable stale-read
counterexamples for the opt-out mode; and a scripted real-network partition test
against the Compose cluster (`make partition-test`). See [PLAN.md](PLAN.md).

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
make test         # cargo test (unit, sim-cluster, fault, jepsen, e2e, 3-process)
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
state is reconstructed from it on startup. Multi-node membership is bootstrapped via
`RUSTKV_NODE_ID` / `RUSTKV_RAFT_LISTEN` / `RUSTKV_PEERS` / `RUSTKV_PEER_CLIENT_URLS` —
`scripts/run-cluster.sh` shows a complete example — and can be changed at runtime
through `/cluster/members` (phase 15).

Logging uses `tracing`; control verbosity with `RUST_LOG` (default `info`), e.g.
`RUST_LOG=debug make run`.

Node-to-node RPCs share one base budget, `RUSTKV_RPC_TIMEOUT_MS` (default 150). Since
phase 20, large transfers no longer have to fit it in one piece: catch-up batches are
capped (`RUSTKV_MAX_APPEND_BYTES`, default 1 MiB), snapshots stream in chunks
(`RUSTKV_SNAPSHOT_CHUNK_BYTES`, default 4 MiB — set to `0` while any pre-phase-20
binary is in the cluster), and bodies over 64 KiB earn transfer time on top of the base
budget (`RUSTKV_ASSUMED_BANDWIDTH`, default 8 MiB/s) — see FAILURE_MODES.md ("Snapshot
/ batch size vs the RPC timeout") for the details.

Known limitations and operational edges are cataloged in **FAILURE_MODES.md**.

## HTTP API

| Method   | Path     | Behavior                                                          |
| -------- | -------- | ----------------------------------------------------------------- |
| `GET`    | `/{key}` | `200` with the stored JSON, `404` if absent. Linearizable by default; `?stale=true` reads locally (fast, may be stale) |
| `PUT`    | `/{key}` | `201` once committed by a majority, `400` if body is not valid JSON |
| `DELETE` | `/{key}` | `204` once committed (idempotent — also `204` if the key was absent) |
| `GET`    | `/cluster/status` | `200` with this node's raft status: `{"id","term","role","leader_id","commit_index","last_log_index"}` (local view, always answers) |

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
- Reads are linearizable by default (ReadIndex, phase 9): the leader confirms it
  still has a majority before answering, so a deposed leader can never serve a
  stale read; non-leaders redirect like writes. `GET /{key}?stale=true` opts out
  and reads the local state machine — fast and always available, but may be stale
  on followers or a just-deposed leader.

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

While a (former) leader is partitioned it answers writes with `504` (until
CheckQuorum deposes it, then `503`) and never commits them; the majority side elects
a successor and keeps serving; on heal, the isolated node's uncommitted entries are
truncated away and it converges. Data lives in named volumes, so
`docker restart rustkv-node1` demonstrates log-replay recovery.

### Scripted partition test

`make partition-test` runs that whole scenario end-to-end
(`scripts/partition-test.sh`, bash + curl only): cluster up → baseline write and
linearizable reads on every node → leader disconnected from `rustkv-raft` → the
isolated node fails writes and linearizable reads with `503`/`504` but still serves
`?stale=true` → the survivors elect and commit a new value → heal (with the
required `--alias`) → all three nodes converge on an equal `commit_index`, the old
leader is demoted, both read modes see the new value, and the doomed isolated write
never committed. Every assertion is a bounded retry loop that dumps the failing
node's logs; there are no bare sleeps.

It needs the Docker daemon, wipes the compose volumes on exit (`down -v`), and
refuses to start if rustkv containers are already running. It is deliberately not
part of `cargo test` (which must stay daemon-free); the deterministic equivalents
of these assertions live in the simulator and Jepsen suites.

## Layout

- `src/lib.rs` — the library: KV state machine (`store`), client HTTP API (`api`);
  later phases add the Raft core, persistence, and the transport trait with real
  (HTTP) and simulated (deterministic, seeded) implementations.
- `src/main.rs` — thin binary: env config, tracing setup, axum server.
- `tests/` — integration tests against a real server on an ephemeral port.
- `CLAUDE.md` — durable engineering rules for this repo (dependency whitelist,
  architecture constraints, standards).
