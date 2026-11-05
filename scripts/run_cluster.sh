#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

MEMBERS="n1@127.0.0.1:7001:8001,n2@127.0.0.1:7002:8002,n3@127.0.0.1:7003:8003"

mkdir -p logs
pids=()

trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT INT TERM

for i in 1 2 3; do
    python -m node --node-id "n$i" --port "700$i" --rpc-port "800$i" \
        --members "$MEMBERS" --replicas 2 --write-acks 1 \
        2>"logs/n$i.log" &
    pids+=($!)
done

echo "cluster up: redis-cli -p 7001 | 7002 | 7003 (Ctrl+C stops all)"

wait
