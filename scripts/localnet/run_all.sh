#!/usr/bin/env bash
# Copyright (c) 2026 Geunhwa Jeong
# SPDX-License-Identifier: Apache-2.0
#
# Full localnet pass: fresh network, publish the engine, start the indexer at the tip, run the
# scenario while it streams, then reconcile the ledger and check the state.
#
#   scripts/localnet/run_all.sh <work dir> <engine checkout copy> <haneul binary> <database url>
#
# The work dir holds the network's config (HANEUL_CONFIG_DIR), the node and indexer logs and the
# package list. The engine checkout is written to (publication records), so use a copy.
# Everything is torn down on exit.

set -euo pipefail

WORK=$(cd "$1" && pwd)
PERP_DEX=$(cd "$2" && pwd)
HANEUL=$3
DATABASE_URL=$4
HERE=$(cd "$(dirname "$0")" && pwd)
INDEXER="$HERE/../../target/debug/perp-indexer"
DB_NAME=${DATABASE_URL##*/}
GRPC=127.0.0.1:9000

export HANEUL_CONFIG_DIR="$WORK/config"
mkdir -p "$HANEUL_CONFIG_DIR"

cleanup() {
    pkill -INT -f "$INDEXER" 2>/dev/null || true
    kill "${NODE_PID:-}" 2>/dev/null || true
}
trap cleanup EXIT

log() { printf '\n== %s\n' "$*"; }

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
    --metrics-address 127.0.0.1:9184 > "$WORK/indexer.log" 2>&1 &
sleep 8

log "running the scenario"
HANEUL=$HANEUL python3 "$HERE/scenario.py" run --perp-dex "$PERP_DEX" | tee "$WORK/scenario.log" | grep -E '^(==|.*checks passed)|FAIL'

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

log "reconciling the ledger"
# shellcheck disable=SC2046
python3 "$HERE/../reconcile_ledger.py" --database-url "$DATABASE_URL" --grpc $GRPC $(cat "$WORK/packages.txt") | tail -3

log "checking the state"
python3 "$HERE/../check_state.py" --database-url "$DATABASE_URL" --grpc $GRPC | tail -3
