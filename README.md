# SelfHeal Cache

A lightweight, self-healing distributed cache built from scratch in Python.

SelfHeal Cache is a learning-oriented project focused on understanding how distributed caching systems work internally, including client-server communication, data storage, TTL expiration, replication, failure detection, and eventual consistency.

The project is being developed incrementally, starting with a working single-node cache and expanding toward a distributed, self-healing architecture.

## Project Goals

- Build a cache server with a Redis-compatible RESP-style client interface.
- Support basic key-value operations.
- Implement TTL-based key expiration.
- Introduce communication between multiple cache nodes.
- Implement replication and eventual consistency.
- Detect node failures and recover from them.
- Explore anti-entropy mechanisms for data synchronization.

## Current Status

**Phase 1 — Single-Node Cache**

| Feature | Status |
|---|---|
| Python project structure | Complete |
| RESP protocol server | Implemented |
| Client connections | Implemented |
| Key-value storage | Implemented |
| TTL expiration | Implemented |
| Unit and integration tests | In progress |
| Initial performance benchmarking | Completed |

Distributed replication, gossip-based failure detection, and anti-entropy synchronization are planned for subsequent phases.

## Tech Stack

- Python 3.12
- Asyncio
- RESP protocol
- Pytest
- Ruff
- Git and GitHub
- WSL2 Ubuntu development environment

## Project Structure

```text
selfheal-cache/
│
├── node/
│   ├── __init__.py
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
│   └── test_store.py
│
├── chaos/
│
├── bench/
│   └── bench.py
│
├── docs/
│   ├── DESIGN.md
│   └── BENCHMARKS.md
│
├── .vscode/
│
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
pip install -e ".[dev]"
```

### 4. Run the cache server

Start a node:

```bash
python -m node --node-id n1 --port 7001 --log-level DEBUG
```

The server starts on port `7001`.

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

### Initial Benchmark Results

Environment: Windows laptop running Ubuntu 24.04 through WSL2, Python 3.12.

| Benchmark | Operations | Throughput |
|---|---:|---:|
| Python benchmark | 200,000 | 30,111 ops/sec |
| Python benchmark (100 clients) | 400,000 | 30,729 ops/sec |
| SET via redis-benchmark | — | 29,931 req/sec |
| GET via redis-benchmark | — | 37,188 req/sec |

These measurements establish the initial single-node performance baseline. Results may vary depending on hardware, system load, and environment.

See [`docs/BENCHMARKS.md`](docs/BENCHMARKS.md) for details.

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

The long-term goal is to develop a distributed cache that can tolerate node failures, synchronize data between available nodes, and recover automatically without requiring a centralized coordinator.

## License

This project is currently under development.