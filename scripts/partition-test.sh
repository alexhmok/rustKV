#!/usr/bin/env bash
# Scripted Docker Compose partition test (phase 17) — automates the manual
# partition walkthrough in the README:
#
#   up -> find leader -> baseline write, linearizable-read everywhere ->
#   disconnect the leader from the raft network (client port stays up) ->
#   isolated node: writes and linearizable reads fail 503-or-504, stale
#   reads still serve the old value -> survivors elect + commit a new
#   write -> heal (MUST re-add the --alias, see compose.yaml header) ->
#   all nodes converge: equal commit_index, ex-leader demoted, both read
#   modes see the new value, the doomed isolated write never committed.
#
# 503 vs 504 on the isolated node is a race, both are correct: 504 while
# it still thinks it leads (write_timeout expires without majority), 503
# after CheckQuorum deposes it (leader_id cleared, no redirect target).
#
# bash + curl only (no jq): /cluster/status has a fixed shape, parsed
# with grep/cut. Every assertion is a bounded retry loop over real time
# (generous deadlines for slow machines, no bare sleeps as
# synchronization); on failure it dumps the failing node's compose logs.
#
# Needs the Docker daemon and ports 8081-8083. Cleanup wipes the compose
# named volumes (down -v), so it REFUSES to start if rustkv containers
# are already running — it never destroys a cluster brought up on purpose.
set -euo pipefail
cd "$(dirname "$0")/.."

KEY=phase17
OLD_VAL='{"v":"before-partition"}'
NEW_VAL='{"v":"after-partition"}'

port_of() { echo $((8080 + $1)); }

# ---- guard: never touch a cluster that is already up ------------------
if [[ -n "$(docker ps --filter name=rustkv-node --format '{{.Names}}')" ]]; then
  echo "rustkv containers are already running; refusing to continue" >&2
  echo "(cleanup runs 'docker compose down -v', destroying their data)." >&2
  echo "Stop them first: docker compose down" >&2
  exit 1
fi

# ---- cleanup + failure diagnostics -------------------------------------
cleanup() { docker compose down -v --timeout 20 >/dev/null 2>&1 || true; }
trap cleanup EXIT

dump_logs() { # $1 = node number, or "all"
  echo "---- docker compose logs ($1) ----" >&2
  if [[ "$1" == all ]]; then
    docker compose logs --no-color 2>/dev/null | tail -n 150 >&2
  else
    docker compose logs --no-color "node$1" 2>/dev/null | tail -n 150 >&2
  fi
}

fail() { # $1 = node whose logs to dump, $2 = message
  echo "FAIL: $2" >&2
  dump_logs "$1"
  exit 1
}

# retry <deadline-secs> <node-for-logs> <description> <predicate...>
# Polls the predicate about once a second until it succeeds; past the
# deadline, dumps the node's logs and exits nonzero.
retry() {
  local deadline=$1 node=$2 desc=$3 start=$SECONDS
  shift 3
  until "$@"; do
    ((SECONDS - start < deadline)) || fail "$node" "$desc (waited ${deadline}s)"
    sleep 1
  done
  echo "    ok: $desc"
}

# ---- fixed-shape JSON helpers (no jq on the whitelist) ------------------
status_json() { curl -s --max-time 5 "localhost:$(port_of "$1")/cluster/status" || true; }
json_num() { grep -o "\"$2\":[0-9]*" <<<"$1" | head -n1 | cut -d: -f2; }
role_of() { grep -o '"role":"[^"]*"' <<<"$(status_json "$1")" | cut -d'"' -f4; }

http_code() { # <method> <node> <path> [curl args...] -> status code, 000 on error
  local method=$1 node=$2 path=$3
  shift 3
  curl -s -o /dev/null -w '%{http_code}' --max-time 15 -X "$method" \
    "localhost:$(port_of "$node")$path" "$@" || true
}

get_body() { # <node> <path> [curl args...] -> body, empty on error
  local node=$1 path=$2
  shift 2
  curl -s --max-time 15 "$@" "localhost:$(port_of "$node")$path" || true
}

# ---- predicates ---------------------------------------------------------
all_status_up() {
  local n
  for n in 1 2 3; do
    grep -q '"role"' <<<"$(status_json "$n")" || return 1
  done
}

LEADER=""
leader_among() { # sets LEADER to the first argument node reporting Leader
  local n
  for n in "$@"; do
    if [[ "$(role_of "$n")" == Leader ]]; then
      LEADER=$n
      return 0
    fi
  done
  return 1
}

not_leader() {
  local role
  role=$(role_of "$1")
  [[ -n "$role" && "$role" != Leader ]]
}

put_ok() { # <node> <key> <json>: 201 through redirects (-L keeps PUT on 307)
  [[ "$(http_code PUT "$1" "/$2" -L -d "$3")" == 201 ]]
}

write_rejected() { # isolated node: PUT must fail 503 (deposed) or 504 (still leading)
  local code
  code=$(http_code PUT "$1" /doomed -d '{"v":"doomed"}')
  [[ "$code" == 503 || "$code" == 504 ]]
}

lin_read_rejected() { # isolated node: default (linearizable) GET fails the same way
  local code
  code=$(http_code GET "$1" "/$KEY")
  [[ "$code" == 503 || "$code" == 504 ]]
}

lin_get_is() { [[ "$(get_body "$1" "/$2" -L)" == "$3" ]]; }
stale_get_is() { [[ "$(get_body "$1" "/$2?stale=true")" == "$3" ]]; }
doomed_absent() { [[ "$(http_code GET "$1" /doomed -L)" == 404 ]]; }

commits_equal() {
  local c1 c2 c3
  c1=$(json_num "$(status_json 1)" commit_index)
  c2=$(json_num "$(status_json 2)" commit_index)
  c3=$(json_num "$(status_json 3)" commit_index)
  [[ -n "$c1" && "$c1" == "$c2" && "$c1" == "$c3" ]]
}

# ---- scenario -----------------------------------------------------------
echo "==> starting the compose cluster (docker compose up --build -d)"
docker compose up --build -d

retry 90 all "all three nodes answer /cluster/status" all_status_up
retry 60 all "a leader emerges" leader_among 1 2 3
echo "==> leader is node$LEADER"

echo "==> baseline write + linearizable reads on every node"
retry 30 "$LEADER" "baseline PUT commits (via node1, following redirects)" \
  put_ok 1 "$KEY" "$OLD_VAL"
for n in 1 2 3; do
  retry 30 "$n" "linearizable GET on node$n sees the baseline" \
    lin_get_is "$n" "$KEY" "$OLD_VAL"
done

echo "==> partitioning node$LEADER off the raft network (client port stays up)"
docker network disconnect rustkv-raft "rustkv-node$LEADER"
OLD_LEADER=$LEADER

# Each rejected attempt may take the node's full 5s write_timeout (and the
# half-open pooled peer connections burn 150ms of rpc_timeout per raft RPC
# — expected phase-16 behavior, not a hang), so keep deadlines generous.
retry 60 "$OLD_LEADER" "PUT on the isolated node fails 503-or-504" \
  write_rejected "$OLD_LEADER"
retry 60 "$OLD_LEADER" "linearizable GET on the isolated node fails 503-or-504" \
  lin_read_rejected "$OLD_LEADER"
retry 30 "$OLD_LEADER" "stale GET on the isolated node still serves the old value" \
  stale_get_is "$OLD_LEADER" "$KEY" "$OLD_VAL"

SURVIVORS=()
for n in 1 2 3; do
  [[ "$n" == "$OLD_LEADER" ]] || SURVIVORS+=("$n")
done

echo "==> waiting for the survivors (nodes ${SURVIVORS[*]}) to elect"
retry 60 all "a new leader emerges among the survivors" leader_among "${SURVIVORS[@]}"
echo "==> new leader is node$LEADER"
retry 30 "$LEADER" "a new PUT commits on the majority side" \
  put_ok "${SURVIVORS[0]}" "$KEY" "$NEW_VAL"

echo "==> healing: reconnecting node$OLD_LEADER (with the required --alias)"
docker network connect --alias "node${OLD_LEADER}-raft" rustkv-raft "rustkv-node$OLD_LEADER"

echo "==> convergence checks"
retry 60 "$OLD_LEADER" "the old leader (node$OLD_LEADER) is no longer Leader" \
  not_leader "$OLD_LEADER"
retry 60 all "commit_index is equal on all three nodes" commits_equal
for n in 1 2 3; do
  retry 30 "$n" "linearizable GET on node$n sees the new value" \
    lin_get_is "$n" "$KEY" "$NEW_VAL"
  retry 30 "$n" "stale GET on node$n sees the new value" \
    stale_get_is "$n" "$KEY" "$NEW_VAL"
done
retry 30 "$OLD_LEADER" "the doomed isolated write never committed (404)" \
  doomed_absent "${SURVIVORS[0]}"

echo "==> PASS: partition test complete (cleanup tears the cluster down)"
