#!/usr/bin/env bash
# Runs a local 3-node rustkv cluster (three processes on localhost).
# Client APIs:  127.0.0.1:8081, :8082, :8083
# Raft RPCs:    127.0.0.1:9081, :9082, :9083
# Data dirs:    ./cluster-data/node{1,2,3}
# Ctrl-C stops all three. Try:
#   curl -i -X PUT localhost:8081/greeting -d '{"hello":"cluster"}'
#   curl -i localhost:8082/greeting
set -euo pipefail
cd "$(dirname "$0")/.."

BIN=${RUSTKV_BIN:-./target/debug/rustkv}
[ -x "$BIN" ] || { echo "binary not found at $BIN — run 'make build' first" >&2; exit 1; }

trap 'kill 0' EXIT INT TERM

for i in 1 2 3; do
  peers=""
  urls=""
  for j in 1 2 3; do
    [ "$j" = "$i" ] && continue
    peers="${peers:+$peers,}$j=127.0.0.1:908$j"
    urls="${urls:+$urls,}$j=http://127.0.0.1:808$j"
  done
  RUSTKV_NODE_ID=$i \
  RUSTKV_LISTEN=127.0.0.1:808$i \
  RUSTKV_RAFT_LISTEN=127.0.0.1:908$i \
  RUSTKV_DATA_DIR=./cluster-data/node$i \
  RUSTKV_PEERS="$peers" \
  RUSTKV_PEER_CLIENT_URLS="$urls" \
  "$BIN" &
done

wait
