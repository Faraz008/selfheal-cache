
# Benchmark Results

## Phase 1 — Initial RESP Server

### Environment

- Date: 03-11-2026
- Machine: Windows laptop (WSL2)
- OS: Ubuntu 24.04
- Python: 3.12
- Server: Single node
- Client port: 7001

### 1. Python Benchmark — Default Configuration

Command:

```bash
python bench/bench.py
```

Results:

| Metric | Result |
|---|---:|
| Total operations | 200,000 |
| Execution time | 6.64 seconds |
| Operations/sec | 30,111 |

### 2. Python Benchmark — 100 Clients

Command:

```bash
python bench/bench.py --clients 100
```

Results:

| Metric | Result |
|---|---:|
| Total operations | 400,000 |
| Execution time | 13.02 seconds |
| Operations/sec | 30,729 |
| Concurrent clients | 100 |

### 3. Redis Benchmark Client Comparison

Command:

```bash
redis-benchmark -p 7001 -t set,get -n 100000 -q
```

Results:

| Operation | Requests/sec | p50 latency |
|---|---:|---:|
| SET | 29,931.16 | 1.631 ms |
| GET | 37,188.55 | 1.319 ms |

Note: redis-benchmark was used as an alternative
benchmark client against the project's RESP server
on port 7001. These results do not represent the
performance of a Redis server.

The initial CONFIG warning is expected because the
custom RESP server does not implement Redis CONFIG.
SET and GET benchmark results were subsequently
reported successfully.

### 4. Performance Summary

| Benchmark | Operations/sec |
|---|---:|
| Python benchmark (default) | 30,111 |
| Python benchmark (100 clients) | 30,729 |
| redis-benchmark SET | 29,931 |
| redis-benchmark GET | 37,189 |

### 5. Observations

- The Python benchmark achieved approximately
  30k operations per second.
- Increasing concurrency to 100 clients resulted
  in a small throughput improvement.
- SET throughput measured approximately 29.9k
  requests per second using redis-benchmark.
- GET throughput measured approximately 37.2k
  requests per second.
- GET throughput was higher than SET throughput
  in this benchmark.
- The implementation meets the initial project
  target of 15k–40k operations per second.

### 6. Conclusion

The single-node RESP cache achieved approximately
30k operations per second in the initial benchmark.

These measurements establish the Phase 1 baseline.
Future cluster implementations will be benchmarked
against these results to evaluate the performance
impact of replication, inter-node communication,
gossip and anti-entropy mechanisms.

### 7. Known Limitations

- Measurements were collected in WSL2.
- Results may vary depending on CPU load and
  available system resources.
- These are initial benchmark results, not
  statistically controlled performance tests.
- Latency percentiles beyond p50 were not recorded.