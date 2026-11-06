# SelfHeal Cache

A lightweight, self-healing distributed cache built from scratch in Python.

SelfHeal Cache is a learning-oriented project focused on understanding how distributed caching systems work internally, including client-server communication, key-value storage, TTL expiration, consistent hashing, node membership, gossip protocols, failure detection, and eventual consistency.

The project is being developed incrementally, starting with a working single-node cache and expanding toward a distributed, self-healing architecture.

## Project Goals

- Build a cache server with a Redis-compatible RESP-style client interface.
- Support basic key-value operations.
- Implement TTL-based key expiration.
- Introduce communication between multiple cache nodes.
- Implement consistent hashing and data distribution.
- Implement replication and eventual consistency.
- Detect node failures through distributed membership protocols.
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
| Failure-detection timing experiment | Completed |

**Failure-detection experiment:** In a five-node cluster, node `n3` was terminated manually. The last healthy node marked it as `DEAD` approximately 9.80 seconds after termination.

Replication, data recovery, and anti-entropy synchronization remain part of the planned development.

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

## Getting Started

### Prerequisites

- Python 3.12
- Git
- Ubuntu/Linux or WSL2

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

The server starts on port `7001`.

You can connect to it using `redis-cli`:

```bash
redis-cli -p 7001
```

Try basic commands:

```redis
SET name "SelfHeal Cache"
GET name
SET temporary "hello" EX 30
TTL temporary
```

## Running a Multi-Node Cluster

The project includes a cluster startup script that launches multiple nodes and uses seed-based discovery to allow nodes to join the cluster.

From the project root, run:

```bash
./scripts/run_cluster.sh 5
```

This starts a five-node cluster.

Each node exposes a client-facing RESP port. The cluster script prints the process information and ports for the launched nodes.

### Inspect Cluster Membership

Connect to one of the nodes:

```bash
redis-cli -p 7001
```

Run:

```redis
CLUSTER NODES
```

The command displays the node membership information, including:

- Node ID
- Node address
- Membership state
- Incarnation number
- `myself` marker for the local node

Example format:

```text
n1 127.0.0.1:7001 alive 1793961276354 myself
n2 127.0.0.1:7002 alive 1793961276360
n3 127.0.0.1:7003 alive 1793961276358
n4 127.0.0.1:7004 alive 1793961276362
n5 127.0.0.1:7005 alive 1793961276358
```

The output above illustrates the format; actual node states and incarnation numbers depend on the running cluster.

## Running Tests

Run the complete test suite:

```bash
pytest -v
```

Run a specific test module:

```bash
pytest -v tests/test_resp.py
```

## Performance Benchmarking

Run the project's Python benchmark:

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

## Benchmark Results

All measurements were collected on a Windows laptop running Ubuntu 24.04 through WSL2, using Python 3.12.

### Phase 1 — Single-Node Performance

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

![Key movement](docs/img/key_movement.png)

Consistent hashing moved approximately 33.8% of keys when increasing from two to three nodes, compared with 66.9% for modulo hashing.

When increasing from ten to eleven nodes, the ring moved approximately 8.8% of keys, compared with 91.1% for modulo hashing.

#### 2. Virtual Node Distribution

The distribution experiment measured load imbalance across 10 physical nodes with different vnode counts.

![Vnode balance](docs/img/vnode_balance.png)

At 150 virtual nodes per physical node, the coefficient of variation was approximately 9.4%. Increasing to 500 vnodes reduced it to 5.5%, with additional ring construction overhead.

The default configuration therefore uses 150 virtual nodes per physical node as a balance between distribution quality and construction cost.

#### 3. Key Distribution

The following chart shows the distribution of 100,000 keys across five physical nodes using 150 virtual nodes per node.

![Key distribution](docs/img/distribution.png)

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
|---|---:|
| Cluster size | 5 nodes |
| Failed node | n3 |
| Failure method | Manual process termination |
| Last healthy node's detection time | 9.798 seconds |

The experiment measured the time between terminating `n3` and the last healthy node marking it as `DEAD`.

The observed detection time was approximately **9.80 seconds**.

This was a single manual failure experiment, not a statistically controlled benchmark. Results may vary with network conditions, system load, and cluster configuration.

See [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) for detailed measurements, methodology, and limitations.

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
- Eventual consistency
- Replication and anti-entropy (planned)

The long-term goal is to develop a distributed cache that can tolerate node failures, synchronize data between available nodes, and recover automatically without requiring a centralized coordinator.

## Documentation

- [`docs/DESIGN.md`](docs/DESIGN.md) — Architecture, design decisions, and system guarantees.
- [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) — Benchmark methodology, results, and observations.

## License

This project is currently under development.