# SelfHeal Cache

A lightweight, self-healing distributed in-memory cache built from scratch in Python.

SelfHeal Cache is a learning-oriented project focused on understanding how distributed caching systems work internally, including client-server communication, key-value storage, TTL expiration, consistent hashing, replication, gossip protocols, failure detection, hinted handoff, read repair, and data rebalancing.

The project is developed incrementally, starting with a working single-node cache and expanding toward a distributed, self-healing architecture.

## Project Goals

- Build a cache server with a Redis-compatible RESP-style client interface.
- Support basic key-value operations and TTL-based key expiration.
- Introduce communication between multiple cache nodes.
- Distribute keys across nodes using consistent hashing.
- Implement replication and eventual consistency.
- Detect node failures through distributed membership protocols.
- Improve availability using sloppy quorum and hinted handoff.
- Improve read latency using hedged reads.
- Repair stale or missing replicas using read repair.
- Support node joining, graceful departure, and historical-data synchronization.
- Explore anti-entropy mechanisms for data synchronization and recovery.

## Current Status

### Phase 1 — Single-Node Cache

| Feature | Status |
|---|---|
| Python project structure | Complete |
| RESP protocol server | Implemented |
| Client connections | Implemented |
| Key-value storage | Implemented |
| TTL expiration | Implemented |
| Initial performance benchmarking | Completed |

### Phase 2 — Consistent Hashing

| Feature | Status |
|---|---|
| Deterministic 64-bit hashing | Implemented |
| Consistent hashing ring | Implemented |
| Virtual nodes | Implemented |
| Binary-search-based key lookup | Implemented |
| Key movement experiments | Completed |
| Vnode distribution experiments | Completed |
| Performance measurements | Completed |

### Phase 3 — Cluster Membership and Failure Detection

| Feature | Status |
|---|---|
| Multi-node cluster startup | Implemented |
| Seed-based node discovery | Implemented |
| Gossip-based membership | Implemented |
| Failure detection | Implemented |
| Suspect and dead node states | Implemented |
| Indirect probing | Implemented |
| Membership inspection using `CLUSTER NODES` | Implemented |
| Failure-detection experiment | Completed |

A five-node experiment was conducted by manually terminating node `n3`. The last healthy node marked it as `DEAD` approximately 9.80 seconds after termination.

### Phase 4 — Replication and Survival

| Feature | Status |
|---|---|
| Versioned records and LWW conflict resolution | Implemented |
| Replication | Implemented |
| Hedged reads | Implemented |
| Configurable read fanout | Implemented |
| Sloppy quorum | Implemented |
| Stand-in node selection | Implemented |
| In-memory hinted handoff | Implemented |
| Hint replay | Implemented |
| Read repair | Implemented |
| Hint capacity limits | Implemented |
| Node-local INFO diagnostics | Implemented |
| Five-node failure/restart chaos test | Passed |

### Phase 5 — [Update with the phase's final status if applicable]

### Phase 6 — Rebalancing and Node Lifecycle

| Feature | Status |
|---|---|
| Pull-based synchronization when a node joins | Implemented |
| Historical-data catch-up for restarted nodes | Implemented |
| JOINING membership state | Implemented |
| JOINING-aware write and read paths | Implemented |
| Graceful node departure using `CLUSTER LEAVE` | Implemented |
| Double-writing during graceful leave | Implemented |
| Explicit permanent removal using `CLUSTER FORGET` | Implemented |
| Manual synchronization using `CLUSTER SYNC` | Implemented |
| Owner-confirmed data cleanup | Implemented |
| Rebalancing diagnostics in INFO | Implemented |
| Join-under-load chaos experiment | Implemented |

Phase 6 introduces data movement when nodes join or leave the cluster. It also adds explicit controls for graceful departure, permanent node removal, and synchronization.

## Tech Stack

- Python 3.12
- Asyncio
- RESP protocol
- TCP networking
- Pytest
- Ruff
- Matplotlib
- Git and GitHub
- WSL2 Ubuntu development environment

## Project Structure

```text
selfheal-cache/
│
├── node/
│   ├── __init__.py
│   ├── ring.py
│   └── ...
│
├── client/
│   ├── __init__.py
│   └── ...
│
├── tests/
│   ├── __init__.py
│   ├── conftest.py
│   ├── test_resp.py
│   ├── test_server.py
│   ├── test_store.py
│   └── ...
│
├── chaos/
│   ├── crude_kill.py
│   └── join_under_load.py
│
├── bench/
│   └── bench.py
│
├── scripts/
│   └── run_cluster.sh
│
├── docs/
│   ├── DESIGN.md
│   ├── BENCHMARKS.md
│   └── img/
│       ├── key_movement.png
│       ├── vnode_balance.png
│       └── distribution.png
│
├── .vscode/
├── .gitignore
├── pyproject.toml
└── README.md
```

*The tree is representative; additional modules and files may exist.*

## Getting Started

### Prerequisites

- Python 3.12
- Git
- Ubuntu/Linux or WSL2
- Optional: `redis-cli`

### 1. Clone the repository

```bash
git clone git@github.com:Faraz008/selfheal-cache.git
cd selfheal-cache
```

### 2. Create a virtual environment

```bash
python3.12 -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

### 3. Install the project

Install the project and development dependencies:

```bash
pip install -e ".[dev,analysis]"
```

### 4. Run a single cache node

```bash
python -m node --node-id n1 --port 7001 --log-level DEBUG
```

The server starts on client port `7001`.

Connect using `redis-cli`:

```bash
redis-cli -p 7001
```

Try basic commands:

```redis
PING
SET name "SelfHeal Cache"
GET name
SET temporary "hello" EX 30
TTL temporary
INFO
```

## Running a Multi-Node Cluster

The project includes a cluster startup script that launches multiple nodes and uses seed-based discovery to allow nodes to join the cluster.

From the project root, run:

```bash
./scripts/run_cluster.sh 5
```

This starts a five-node cluster.

Each node exposes a client-facing RESP port. The cluster script prints the process information and ports for the launched nodes.

**Important:** Starting several nodes with only different `--node-id` and `--port` arguments does not necessarily make them members of the same cluster. Use the supplied cluster script or provide the correct peer/seed configuration.

## Inspect Cluster Membership

Connect to one of the nodes:

```bash
redis-cli -p 7001
```

Run:

```redis
CLUSTER NODES
```

The command displays membership information, including:

- Node ID
- Node address
- Membership state
- Incarnation number
- `myself` marker for the local node

Example output format:

```text
n1 127.0.0.1:7001 alive 1793961276354 myself
n2 127.0.0.1:7002 alive 1793961276360
n3 127.0.0.1:7003 alive 1793961276358
n4 127.0.0.1:7004 alive 1793961276362
n5 127.0.0.1:7005 alive 1793961276358
```

The output above illustrates the format. Actual node states and incarnation numbers depend on the running cluster.

## Node Lifecycle and Rebalancing

Phase 6 introduces explicit commands for managing node departure and synchronizing data across the cluster.

### Graceful Node Departure — `CLUSTER LEAVE`

To gracefully remove a node from the cluster, connect to that node and run:

```redis
CLUSTER LEAVE
```

The node enters the `LEAVING` state and begins handing its data to the nodes that will own the affected key ranges after it leaves.

During this process:

- The departing node remains part of the transition.
- Peers double-write to the future owners on a best-effort basis.
- The departing node pushes its existing records to future owners.
- The node transitions to `LEFT` after successful handoff.

If the handoff fails or times out, the node reverts to `ALIVE` with a bumped incarnation, allowing the operation to be retried.

A node that crashes during departure can remain in `LEAVING`. It can be restarted to continue recovery or permanently removed using `CLUSTER FORGET`.

### Permanent Node Removal — `CLUSTER FORGET <id>`

Use `CLUSTER FORGET` when a node is permanently unavailable and should be explicitly removed from cluster membership.

Run this command on a surviving node:

```redis
CLUSTER FORGET n2
```

Replace `n2` with the ID of the node being removed.

This is a forced membership removal. It does not automatically redistribute the removed node's data or guarantee that the remaining nodes have restored the configured replication factor.

After forgetting a permanently dead node, run `CLUSTER SYNC` on surviving nodes to synchronize data and restore the desired replica placement.

### Synchronization — `CLUSTER SYNC`

Run:

```redis
CLUSTER SYNC
```

This triggers synchronization of data with the current cluster ownership and replication layout.

It is useful after:

- A node joins but reaches `ALIVE` with incomplete synchronization.
- A node is permanently removed using `CLUSTER FORGET`.
- A previous synchronization attempt could not complete because some sources were unavailable.

Synchronization uses available replicas as sources. If a source becomes unavailable, another candidate can be tried. If synchronization cannot complete within the configured join wait period, the node can become `ALIVE` with gaps rather than remaining unavailable indefinitely.

Read repair and subsequent synchronization can help recover missing records.

**Important:** These operations do not provide disk durability. Records and synchronization sessions are held in memory.

## Node Diagnostics

The custom `INFO` command provides diagnostic information about the node being queried.

Connect to a node:

```bash
redis-cli -p 7001 INFO
```

The command reports node-local information.

### Core fields

| Field | Description |
|---|---|
| `node_id` | ID of the current node |
| `live_keys` | Number of currently live keys |
| `raw_records` | Number of raw records in local storage |
| `hints_held` | Number of hints currently held by this node |
| `hints_rejected` | Number of hints rejected because of capacity limits |
| `hints_written` | Number of hints successfully written by this node |
| `read_repairs` | Number of read-repair operations recorded by this node |

### Rebalancing and lifecycle fields

| Field | Description |
|---|---|
| `state` | Current membership/lifecycle state of the local node |
| `ring_nodes` | Number of nodes currently participating in the ownership ring |
| `keys_received` | Number of records received through rebalancing/synchronization |
| `keys_sent` | Number of records sent during synchronization or handoff |
| `keys_pushed` | Number of records pushed to future owners during graceful leave |
| `keys_cleaned` | Number of records cleaned up after ownership transfer was confirmed |

These fields help inspect node lifecycle transitions and observe data movement during joining, leaving, and cleanup.

All `INFO` counters are node-local metrics, not cluster-wide totals. A counter may remain zero until its corresponding event occurs. The exact values depend on the node's activity and lifecycle.

## Client Usage

The server exposes a RESP-compatible interface and can be used with compatible clients.

The following example uses Python's `redis-py` library:

```python
import redis

cache = redis.Redis(
    host="127.0.0.1",
    port=7001,
    decode_responses=True
)

cache.ping()
cache.set("language", "Python")
print(cache.get("language"))

cache.set("session", "active", ex=60)
print(cache.ttl("session"))

cache.delete("session")
cache.close()
```

This demonstrates compatibility through a standard RESP client. It does not demonstrate the API of the project's separate `client/` package.

## Running Tests

Run the complete test suite:

```bash
pytest -v
```

Run a specific test module:

```bash
pytest -v tests/test_resp.py
```

Or run the server tests:

```bash
pytest -v tests/test_server.py
```

## Performance Benchmarking

The project's Python benchmark connects to an already-running server. It does not start the server itself.

Start a node first:

```bash
python -m node --node-id n1 --port 7001
```

In another terminal, run the default benchmark:

```bash
python bench/bench.py
```

Run with 100 concurrent clients:

```bash
python bench/bench.py --clients 100
```

You can also use `redis-benchmark` as an alternative benchmark client against the project's RESP server:

```bash
redis-benchmark -p 7001 -t set,get -n 100000 -q
```

This command benchmarks SelfHeal Cache on port `7001`; it does not benchmark a Redis server.

For distributed testing, start the multi-node cluster and verify membership before running the benchmark. Record the cluster size, node configuration, and benchmark target alongside the results.

See `docs/BENCHMARKS.md` for detailed measurements, methodology, and limitations.

## Benchmark Results

All measurements were collected on a Windows laptop running Ubuntu 24.04 through WSL2, using Python 3.12.

### Phase 1 — Initial Single-Node Performance

| Benchmark | Operations | Throughput |
|---|---:|---:|
| Python benchmark | 200,000 | 30,111 ops/sec |
| Python benchmark (100 clients) | 400,000 | 30,729 ops/sec |
| SET via redis-benchmark | — | 29,931 req/sec |
| GET via redis-benchmark | — | 37,188 req/sec |

These measurements establish the initial single-node performance baseline. Results may vary depending on hardware, system load, and environment.

### Phase 2 — Consistent Hashing Experiments

The consistent hashing implementation was evaluated using 100,000 keys and varying numbers of physical and virtual nodes.

#### 1. Key Movement

The experiment compared modulo hashing against consistent hashing when adding one physical node.

**Consistent hashing moved approximately 33.8% of keys when increasing from two to three nodes**, compared with 66.9% for modulo hashing.

When increasing from ten to eleven nodes, the ring moved approximately 8.8% of keys, compared with 91.1% for modulo hashing.

#### 2. Virtual Node Distribution

The distribution experiment measured load imbalance across 10 physical nodes with different vnode counts.

At 150 virtual nodes per physical node, the coefficient of variation was approximately 9.4%. Increasing to 500 vnodes reduced it to 5.5%, with additional ring construction overhead.

The default configuration therefore uses 150 virtual nodes per physical node as a balance between distribution quality and construction cost.

#### 3. Key Distribution

The following chart shows the distribution of 100,000 keys across five physical nodes using 150 virtual nodes per node.

See the distribution charts in `docs/img/`.

#### Phase 2 Performance

| Metric | Result |
|---|---:|
| Physical nodes | 10 |
| Virtual nodes per physical node | 150 |
| Total virtual nodes | 1,500 |
| Ring construction time | 9.3 ms |
| Average lookup time | 1.48 μs |
| Keys evaluated | 100,000 |

The lookup measurement includes key hashing and ring lookup, not network communication or replication.

### Phase 3 — Failure Detection

A five-node cluster was used to evaluate gossip-based failure detection.

| Metric | Result |
|---|---|
| Cluster size | 5 nodes |
| Failed node | n3 |
| Failure method | Manual process termination |
| Last healthy node's detection time | 9.798 seconds |

The experiment measured the time between terminating `n3` and the last healthy node marking it as `DEAD`.

The observed detection time was approximately 9.80 seconds.

This was a single manual failure experiment, not a statistically controlled benchmark. Results may vary with network conditions, system load, and cluster configuration.

### Phase 4 — Replication and Survival

#### Single-Node Performance

The benchmark was repeated after introducing the Phase 4 replication and read-coordination changes.

| Benchmark | Operations | Throughput |
|---|---:|---:|
| Python benchmark | 200,000 | 20,623 ops/sec |
| Python benchmark (100 clients) | 400,000 | 19,960 ops/sec |

Compared with the Phase 1 baseline, the measured throughput decreased by approximately 31.5% in the default run and 35.1% in the 100-client run.

**Important:** This was a single-node benchmark. The running membership contained only `n1`, even though the configuration specified a replication factor of three. It does not measure multi-node RPC traffic, stand-in writes, or distributed replication performance.

These results show a single-node performance regression after the Phase 4 changes. They should not be interpreted as a distributed-cluster performance measurement.

#### Five-Node Failure and Restart Experiment

The chaos script was used to test a node failure and restart scenario.

| Metric | Result |
|---|---:|
| Cluster size | 5 nodes |
| Failed node | n2 |
| Requests | 237,908 |
| Acknowledged writes | 118,954 |
| Failed requests | 0 |
| Client retries | 5 |
| Lost acknowledged writes | 0 |
| Latency p50 | 1.3 ms |
| Latency p99 | 3.6 ms |
| Maximum latency | 35 ms |
| Result | PASS |

The experiment terminated `n2`, restarted it, and checked acknowledged writes for loss.

The test passed its acknowledged-write verification for this particular failure and recovery scenario. It does not establish durability across process restarts: storage and hints are in-memory.

See `docs/BENCHMARKS.md` for detailed measurements, methodology, and limitations.

## Failure and Recovery Testing

### Five-Node Failure and Restart Experiment

Run:

```bash
python chaos/crude_kill.py
```

The script starts a five-node cluster using client ports `7101–7105`, terminates `n2`, and restarts it.

A recorded run produced:

```text
requests=237908  acked_writes=118954
failed_requests=0  client_retries=5
lost_acked_writes=0
latency p50=1.3ms  p99=3.6ms  max=35ms
PASS
```

The script checks whether acknowledged writes survive the tested node failure and restart scenario.

### Join Under Load Experiment

Run:

```bash
python chaos/join_under_load.py
```

This experiment tests node joining and historical-data synchronization while the cluster is handling client traffic.

It exercises the rebalancing path under concurrent load, including:

- Introducing a new node while requests are being processed.
- Synchronizing historical records to the joining node.
- Handling ownership changes while writes continue.
- Observing request failures and retries during the join.
- Checking data movement and synchronization diagnostics.

The experiment helps evaluate whether the cluster can add capacity without unnecessarily interrupting client traffic.

A successful run demonstrates the tested join-under-load scenario; it does not establish a general guarantee of zero data loss or zero downtime under every network partition or failure combination.

## Important Design Limitations

- The system is leaderless; any node can coordinate a client request.
- LWW conflict resolution uses `(timestamp_ms, node_id)`.
- TTL is represented as an absolute Unix timestamp in milliseconds.
- Hints are stored in memory and can be lost if a stand-in fails before replay.
- Storage is in-memory; there is no disk persistence.
- A restarted node must synchronize historical data before it can fully restore its previous replica coverage.
- If synchronization reaches `join_max_wait_ms` without completing, a joining node can become `ALIVE` with gaps. Read repair and `CLUSTER SYNC` can help heal those gaps.
- Read repair can restore data only for keys that are read.
- A sloppy-quorum write acknowledged through a stand-in may not be visible to a quorum read over the original preference list until hint replay or repair.
- `SET NX` and `SET XX` are best-effort read-then-write operations, not atomic cluster-wide operations.
- Rebalancing synchronization sessions are in-memory and are not resumable after process loss.
- `CLUSTER FORGET` removes a node from membership but does not automatically restore the desired replication factor. Synchronization on surviving nodes is required.
- The project does not currently provide transactions, authentication, or TLS.

## Development

Run lint checks:

```bash
ruff check .
```

Check formatting:

```bash
ruff format --check .
```

Format the project:

```bash
ruff format .
```

## Design Philosophy

The project is being built incrementally, prioritizing understanding and correctness before optimization.

The architecture explores distributed-systems concepts such as:

- Leaderless coordination
- Consistent hashing and virtual nodes
- Gossip-based membership
- Failure detection and node state transitions
- Replication and eventual consistency
- Hedged reads
- Sloppy quorum and hinted handoff
- Read repair
- Pull-based synchronization and graceful rebalancing
- Historical data recovery

The long-term goal is to develop a distributed cache that can tolerate node failures, synchronize data between available nodes, and recover automatically without requiring a centralized coordinator.

## Documentation

- `docs/DESIGN.md` — Architecture, guarantees, failure handling, rebalancing, and design decisions.
- `docs/BENCHMARKS.md` — Benchmark methodology, measurements, and observations.

## License

This project is currently under development.