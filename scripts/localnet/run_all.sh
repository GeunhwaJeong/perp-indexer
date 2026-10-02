#!/usr/bin/env bash
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0
#
# Full localnet pass: fresh network, publish the engine, start the indexer at the tip and the API
# over it, run the scenario while both follow the chain, then reconcile the ledger, check the
# state and check the API.
#
#   scripts/localnet/run_all.sh <work dir> <engine checkout copy> <haneul binary> <database url>
#
# The work dir holds the network's config (HANEUL_CONFIG_DIR), the logs and the package list.
# The engine checkout is written to (publication records), so use a copy. Build the binaries
# first (`cargo build`). Needs grpcurl, psql and Node 22 or later. Everything is torn down on
# exit; set HOLD to a path to keep the stack up after the checks until that file appears.

set -euo pipefail

WORK=$(cd "$1" && pwd)
PERP_DEX=$(cd "$2" && pwd)
HANEUL=$3
DATABASE_URL=$4
HERE=$(cd "$(dirname "$0")" && pwd)
INDEXER="$HERE/../../target/debug/perp-indexer"
API="$HERE/../../target/debug/perp-api"
API_URL=http://127.0.0.1:3002
DB_NAME=${DATABASE_URL##*/}
GRPC=127.0.0.1:9000

export HANEUL_CONFIG_DIR="$WORK/config"
mkdir -p "$HANEUL_CONFIG_DIR"

cleanup() {
    kill "${CHECK_PID:-}" "${SCENARIO_PID:-}" 2>/dev/null || true
    pkill -INT -f "$API" 2>/dev/null || true
    pkill -INT -f "$INDEXER" 2>/dev/null || true
    kill "${NODE_PID:-}" 2>/dev/null || true
}
trap cleanup EXIT

log() { printf '\n== %s\n' "$*"; }

# Waits for a file, giving up if the process that should write it has died.
await_file() {
    until [ -e "$1" ]; do
        kill -0 "$2" 2>/dev/null || { echo "$1 never appeared"; exit 1; }
        sleep 0.5
    done
}

log "starting a fresh local network"
"$HANEUL" start --with-faucet --force-regenesis > "$WORK/node.log" 2>&1 &
NODE_PID=$!
until lsof -iTCP:9000 -sTCP:LISTEN > /dev/null 2>&1; do sleep 1; done
sleep 4
if ! "$HANEUL" client envs 2>/dev/null | grep -q local; then
    "$HANEUL" client -y envs > /dev/null 2>&1 || true
    "$HANEUL" client new-env --alias local --rpc "http://$GRPC" > /dev/null
fi
"$HANEUL" client switch --env local > /dev/null
"$HANEUL" client faucet > /dev/null
sleep 3

log "publishing the engine"
HANEUL=$HANEUL python3 "$HERE/scenario.py" publish --perp-dex "$PERP_DEX" | tail -1 > "$WORK/packages.txt"
PACKAGES=$(sed 's/--package //g; s/ /,/g' "$WORK/packages.txt")

log "starting the indexer at the tip"
dropdb --if-exists "$DB_NAME"
createdb "$DB_NAME"
PERP_PACKAGES="$PACKAGES" "$INDEXER" --database-url "$DATABASE_URL" \
    --rpc-api-url "http://$GRPC" --streaming-url "http://$GRPC" \
    --pnl-tick-interval-ms "${PNL_TICK_INTERVAL_MS:-5000}" \
    --metrics-address 127.0.0.1:9184 > "$WORK/indexer.log" 2>&1 &
sleep 8

log "running the scenario"
rm -f "$WORK"/deployment.json "$WORK"/gate "$WORK"/ready "$WORK"/stop "$WORK"/probe.json
HANEUL=$HANEUL python3 "$HERE/scenario.py" run --perp-dex "$PERP_DEX" \
    --deployment "$WORK/deployment.json" --gate "$WORK/gate" --probe "$WORK/probe.json" \
    > "$WORK/scenario.log" 2>&1 &
SCENARIO_PID=$!

# The scenario stops once the market exists. The API starts from the deployment file it wrote,
# and a client subscribes to every channel before anything trades.
await_file "$WORK/deployment.json" $SCENARIO_PID
"$API" --database-url "$DATABASE_URL?application_name=perp-api" --deployment "$WORK/deployment.json" \
    --listen-address 127.0.0.1:3002 --metrics-address 127.0.0.1:9185 > "$WORK/api.log" 2>&1 &
until curl -sf "$API_URL/health" > /dev/null; do sleep 0.5; done
node "$HERE/../check_api.mjs" --api "$API_URL" --database-url "$DATABASE_URL" \
    --deployment "$WORK/deployment.json" --probe "$WORK/probe.json" \
    --ready "$WORK/ready" --stop "$WORK/stop" > "$WORK/api_check.log" 2>&1 &
CHECK_PID=$!
await_file "$WORK/ready" $CHECK_PID
touch "$WORK/gate"
SCENARIO_STATUS=0
wait $SCENARIO_PID || SCENARIO_STATUS=$?
grep -E '^(==|.*checks passed)|FAIL' "$WORK/scenario.log" || true
[ $SCENARIO_STATUS -eq 0 ] || { tail -20 "$WORK/scenario.log"; exit 1; }

log "waiting for the indexer to reach the tip"
TIP=$(grpcurl -plaintext -d '{}' $GRPC haneul.rpc.v2.LedgerService/GetServiceInfo | python3 -c 'import json,sys; print(json.load(sys.stdin)["checkpointHeight"])')
for _ in $(seq 1 60); do
    STATE=$(psql "$DATABASE_URL" -AtXc "SELECT coalesce(min(checkpoint_hi_inclusive), -1) FROM watermarks")
    [ "$STATE" -ge "$TIP" ] && break
    sleep 1
done
echo "tip $TIP, watermark $STATE"
grep -c 'streaming' "$WORK/indexer.log" | sed 's/^/streaming log lines: /'
grep -E 'ERROR|WARN' "$WORK/indexer.log" | head -5 || true

log "checking the API"
for _ in $(seq 1 60); do
    SERVED=$(curl -sf "$API_URL/v4/height" | python3 -c 'import json,sys; print(json.load(sys.stdin)["height"])')
    [ "$SERVED" -ge "$TIP" ] && break
    sleep 1
done
touch "$WORK/stop"
CHECK_STATUS=0
wait $CHECK_PID || CHECK_STATUS=$?
grep -E 'FAIL|checks passed|^messages' -A2 "$WORK/api_check.log" | head -60 || true
grep -E 'ERROR|WARN' "$WORK/api.log" | head -5 || true

log "reconciling the ledger"
# shellcheck disable=SC2046
python3 "$HERE/../reconcile_ledger.py" --database-url "$DATABASE_URL" --grpc $GRPC $(cat "$WORK/packages.txt") | tail -3

log "checking the state"
python3 "$HERE/../check_state.py" --database-url "$DATABASE_URL" --grpc $GRPC | tail -3

if [ -n "${HOLD:-}" ]; then
    log "holding the stack until $HOLD appears"
    until [ -e "$HOLD" ]; do sleep 1; done
fi

[ $CHECK_STATUS -eq 0 ] || exit 1
