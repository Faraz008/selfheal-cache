#!/usr/bin/env bash
# Cluster control + fault injection. One implementation, used by humans and by chaos/*.py.
set -euo pipefail
cd "$(dirname "$0")/.."
DC="docker compose"
nip() { docker inspect -f '{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}' "$($DC ps -q "$1")"; }

cmd="${1:-help}"; shift || true
case "$cmd" in
  up)      $DC up -d --build --wait --wait-timeout 120 n1 n2 n3 n4 n5 ;;
  down)    $DC --profile '*' down -v --remove-orphans ;;
  add)     $DC up -d --wait --wait-timeout 120 "$@" ;;       # e.g. add n6
  kill)    $DC kill "$@" ;;                                  # SIGKILL: a crash
  stop)    $DC stop "$@" ;;                                  # SIGTERM
  start)   $DC start "$@" ;;
  pause)   $DC pause "$@" ;;                                 # freeze: process alive, silent
  unpause) $DC unpause "$@" ;;
  split)   # split "n1 n2 n3" "n4 n5": drop all traffic between the two groups, both directions.
           # Only the client port from the host stays reachable.
    for a in $1; do for b in $2; do
      ip="$(nip "$b")"
      $DC exec -T "$a" iptables -A INPUT  -s "$ip" -j DROP
      $DC exec -T "$a" iptables -A OUTPUT -d "$ip" -j DROP
    done; done ;;
  heal)    for n in $($DC ps --services --status running); do $DC exec -T "$n" iptables -F; done ;;
  status)  for p in 7001 7002 7003 7004 7005 7006; do
             if redis-cli -p "$p" CLUSTER NODES 2>/dev/null; then break; fi
           done ;;
  *) echo "usage: $0 up|down|add|kill|stop|start|pause|unpause|split|heal|status [nodes...]"; exit 1 ;;
esac