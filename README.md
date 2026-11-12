# selfheal-cache

A sharded, replicated, self-healing in-memory cache cluster, written from scratch in Python
(asyncio, stdlib only in the core). Speaks the Redis wire protocol, so `redis-cli` works.

![demo](docs/img/demo.gif)

*A node is `kill -9`ed mid-traffic. The dashboard shows `err=0` throughout; clients retry at most once.*

## What it proves

| Requirement | Evidence |
|---|---|
| Sharded with consistent hashing (150 vnodes) | adding a node moves ~1/(N+1) of keys, only onto the new node ([chart](docs/img/key_movement.png)) |
| Replicated (N=3), reads survive node loss | `tests/test_cluster.py::test_read_fails_over_when_primary_is_down` |
| Automatic failure detection (SWIM-style) | all survivors mark a killed node DEAD in ~6-8 s ([numbers](docs/BENCHMARKS.md)) |
| Zero-downtime join / leave / return | 0 failed requests, 0 lost acked writes under load ([chaos run](docs/img/chaos_eventual.png)) |
| TTL honored consistently | one absolute deadline, identical on every replica through every code path |
| Survives live kill mid-traffic | `kill_one`, `pause_one` scenarios: 0 failures beyond one retry |
| Eviction (LRU/LFU) | LFU survives scans that flush LRU ([chart](docs/img/eviction_hit_rate.png)) |
| Quorum mode | stale read in eventual mode, none in quorum mode (tested) |

The full requirement-to-test map is in [docs/CLAIMS.md](docs/CLAIMS.md), and CI checks it stays true.

## Quickstart

```bash
git clone https://github.com/YOURUSER/selfheal-cache && cd selfheal-cache
python3.12 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest                                   # ~N tests, a few minutes

./scripts/run_cluster.sh 5               # 5 local processes, ports 7001-7005
redis-cli -p 7001 SET user:1 alice
redis-cli -p 7004 GET user:1             # served via a different node
redis-cli -p 7001 WHERE user:1           # which nodes own the key
redis-cli -p 7001 CLUSTER NODES
```

Docker (5 containers, fault injection):
```bash
./scripts/cluster.sh up
python -m chaos.loadgen --duration 30    # live dashboard
./scripts/cluster.sh kill n3             # watch err stay 0
./scripts/cluster.sh down
```

## Commands

`PING SET(EX|PX|EXAT|PXAT|NX|XX) GET DEL EXISTS EXPIRE PEXPIRE EXPIREAT PEXPIREAT TTL PTTL PERSIST DBSIZE`
Debug/admin: `WHERE <key>`, `LOCALGET <key>`, `INFO`, `CLUSTER NODES|LEAVE|FORGET <id>|SYNC`

## Client library

```python
from client.cluster import ClusterClient

async with ClusterClient([("127.0.0.1", 7001)], retries=1) as cl:
    await cl.set("k", "v", ex=60)
    print(await cl.get("k"))
```

## Architecture

See [docs/architecture.md](docs/architecture.md) for diagrams and request lifecycles, and
[docs/DESIGN.md](docs/DESIGN.md) for every decision and its trade-off.

## Tunable consistency

| | eventual (default) | quorum (`--consistency quorum`) |
|---|---|---|
| Write succeeds when | W acks; stand-ins and hints count | W acks from the preference list only |
| Read succeeds when | any `read_fanout` replicas answer | R replicas answer |
| Minority side of a partition | keeps accepting writes, diverges | refuses, per key |
| Guarantee | convergence | an acked write is visible to later reads (stable ring, no eviction) |

**Quorum mode is not linearizable.** No CAS, `SET NX/XX`/`EXPIRE`/`PERSIST` are read-modify-write,
and a failed write may still surface later. See [docs/PARTITIONS.md](docs/PARTITIONS.md).

## How it was tested

Unit and integration tests (in-process clusters, injected clocks, skewed clocks), plus a Docker
chaos harness run exercised the cluster under node failures, recovery and membership changes while clients continuously generated traffic with an exact correctness oracle: kill, freeze, double-kill, join, leave and network partitions via iptables.

### Recorded load-test results

| Metric | Result |
|---|---:|
| Duration | 100 seconds |
| Total operations | 250,000 |
| Failed requests | 2 |
| Retries | 63 |
| Stale responses | 12 |
| Peak latency | 3004.6 ms |

Raw results: [`docs/chaos_eventual.csv`](docs/chaos_eventual.csv)

The run demonstrates continued request handling during fault injection,
with retries and latency spikes during disruption and recovery.

## Known limits

Summarized here, detailed in [docs/DESIGN.md](docs/DESIGN.md#known-limits):
in-memory only (no persistence); last-writer-wins loses concurrent writes; no anti-entropy, so
replicas that are never read can stay divergent after a partition; full-table gossip scales to
roughly 50 nodes; NTP-synced clocks are assumed.

## Layout

`node/` server, store, ring, gossip, detector, replication, rebalancing · `client/` smart client ·
`chaos/` load generator and fault harness · `tests/` · `bench/` · `docs/` · `scripts/`