#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
N=${1:-5}
mkdir -p logs
pids=()
trap 'kill "${pids[@]}" 2>/dev/null || true' EXIT INT TERM
for i in $(seq 1 "$N"); do
  args=(--node-id "n$i" --port "700$i" --rpc-port "800$i" --replicas 3)
  if [ "$i" -gt 1 ]; then args+=(--seeds 127.0.0.1:8001); fi
  python -m node "${args[@]}" 2>"logs/n$i.log" &
  pids+=($!)
  echo "n$i pid=$!"
done
echo "cluster up on 7001..700$N (Ctrl+C stops all)"
wait || true