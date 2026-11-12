#!/usr/bin/env bash
set -uo pipefail
echo "# 5-node cluster, replicas=3, write_acks=2"
redis-cli -p 7001 CLUSTER NODES
python -m chaos.loadgen --duration 40 --csv /tmp/demo.csv &
LG=$!
sleep 12
echo; echo ">>> kill -9 n3 (a crash, no goodbye)"
./scripts/cluster.sh kill n3 >/dev/null
sleep 14
echo; echo ">>> restart n3"
./scripts/cluster.sh start n3 >/dev/null
wait $LG
echo; echo "# final view"
redis-cli -p 7001 CLUSTER NODES