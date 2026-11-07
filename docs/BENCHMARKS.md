# SelfHeal Cache — Benchmark Results

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

Note: redis-benchmark was used as an alternative benchmark client against the project's RESP server on port 7001. These results do not represent the performance of a Redis server.

The initial CONFIG warning is expected because the custom RESP server does not implement Redis CONFIG. SET and GET benchmark results were subsequently reported successfully.

### 4. Performance Summary

| Benchmark | Operations/sec |
|---|---:|
| Python benchmark (default) | 30,111 |
| Python benchmark (100 clients) | 30,729 |
| redis-benchmark SET | 29,931 |
| redis-benchmark GET | 37,189 |

### 5. Observations

- The Python benchmark achieved approximately 30k operations per second.
- Increasing concurrency to 100 clients resulted in a small throughput improvement.
- SET throughput measured approximately 29.9k requests per second using redis-benchmark.
- GET throughput measured approximately 37.2k requests per second.
- GET throughput was higher than SET throughput in this benchmark.
- The implementation meets the initial project target of 15k–40k operations per second.

### 6. Conclusion

The single-node RESP cache achieved approximately 30k operations per second in the initial benchmark.

These measurements establish the Phase 1 baseline. Future cluster implementations will be benchmarked against these results to evaluate the performance impact of replication, inter-node communication, gossip and anti-entropy mechanisms.

### 7. Known Limitations

- Measurements were collected in WSL2.
- Results may vary depending on CPU load and available system resources.
- These are initial benchmark results, not statistically controlled performance tests.
- Latency percentiles beyond p50 were not recorded.

---

## Phase 2 — Consistent Hashing

### Environment and Configuration

- Date: 04-11-2026
- Machine: Windows laptop (WSL2)
- OS: Ubuntu 24.04
- Python: 3.12
- Hash function: MD5 (64-bit hash values)
- Physical nodes: 10 for vnode distribution experiments
- Keys evaluated: 100,000
- Default virtual nodes: 150 per physical node

### 1. Key Movement — Modulo vs Consistent Hashing

The experiment measured the percentage of keys that change owners when adding one physical node.

The same 100,000 keys were evaluated against both configurations.

| Nodes Before Addition | Modulo Movement | Ring Movement | Ideal Ring Movement |
|---:|---:|---:|---:|
| 2 | 66.9% | 33.8% | 33.3% |
| 3 | 75.1% | 24.4% | 25.0% |
| 4 | 79.9% | 19.8% | 20.0% |
| 5 | 83.6% | 15.9% | 16.7% |
| 6 | 85.7% | 14.1% | 14.3% |
| 7 | 87.4% | 11.2% | 12.5% |
| 8 | 88.8% | 9.9% | 11.1% |
| 9 | 90.0% | 9.2% | 10.0% |
| 10 | 91.1% | 8.8% | 9.1% |

**Observations:**

- Modulo hashing moved approximately 66.9% of keys when increasing from two to three nodes.
- Consistent hashing moved approximately 33.8% in the same scenario.
- When increasing from ten to eleven nodes, modulo hashing moved 91.1% of keys.
- The ring moved only 8.8%, close to the theoretical 9.1%.

Consistent hashing therefore significantly reduces unnecessary key redistribution during membership changes.

### 2. Virtual Node Distribution

The experiment measured load balance across 10 physical nodes using different vnode counts and 100,000 keys.

The coefficient of variation (CV) was used to measure load imbalance:

CV = Standard Deviation / Mean

A lower CV indicates a more balanced distribution.

| Vnodes per Node | CV | Maximum / Mean |
|---:|---:|---:|
| 1 | 69.4% | 2.17 |
| 5 | 50.7% | 1.93 |
| 10 | 32.5% | 1.78 |
| 25 | 15.9% | 1.33 |
| 50 | 18.2% | 1.27 |
| 100 | 11.9% | 1.24 |
| 150 | 9.4% | 1.14 |
| 200 | 8.3% | 1.10 |
| 500 | 5.5% | 1.11 |

**Observations:**

- A single vnode per physical node produced significant load imbalance.
- Increasing vnode counts generally improved distribution.
- At 150 vnodes, the coefficient of variation was approximately 9.4%.
- Increasing to 500 vnodes reduced CV further to 5.5%.
- Improvements are not strictly monotonic because of statistical variation.

The default configuration was set to 150 virtual nodes per physical node, balancing distribution quality and ring construction overhead.

### 3. Ring Construction and Lookup Performance

The experiment measured ring construction time and average key lookup time for different vnode counts.

Configuration:

- 10 physical nodes
- 100,000 keys
- MD5-based 64-bit hashing
- Binary search using `bisect`

Results:

| Vnodes per Node | Total Tokens | Build Time (ms) | Lookup (μs/key) |
|---:|---:|---:|---:|
| 1 | 10 | 0.1 | 1.35 |
| 5 | 50 | 0.3 | 1.38 |
| 10 | 100 | 0.7 | 1.37 |
| 25 | 250 | 1.6 | 1.39 |
| 50 | 500 | 3.0 | 1.45 |
| 100 | 1,000 | 6.2 | 1.47 |
| 150 | 1,500 | 9.3 | 1.48 |
| 200 | 2,000 | 12.9 | 1.51 |
| 500 | 5,000 | 34.0 | 1.54 |

**Default Configuration — 150 Vnodes**

| Metric | Result |
|---|---:|
| Physical nodes | 10 |
| Virtual nodes per physical node | 150 |
| Total virtual tokens | 1,500 |
| Ring construction time | 9.3 ms |
| Average lookup time | 1.48 μs |
| Keys evaluated | 100,000 |

**Observations:**

- Ring construction time increases with the total number of virtual nodes.
- Lookup time remains relatively stable as vnode count increases.
- At 150 vnodes, average lookup time was approximately 1.48 microseconds.
- Lookup time includes MD5 hashing and binary search.
- Ring reconstruction is required only when cluster membership changes, rather than on every request.

### 4. Conclusion

The Phase 2 experiments demonstrate that consistent hashing significantly reduces key movement compared with modulo hashing.

Virtual nodes improve key distribution across physical nodes, while binary-search-based lookups maintain low lookup overhead.

The implementation uses 150 virtual nodes per physical node as its default configuration.

The measured construction time was 9.3 ms for 10 physical nodes, and average lookup time was 1.48 μs per key.

These results establish the consistent hashing performance baseline before integrating the ring with the distributed cluster in subsequent phases.

### 5. Known Limitations

- Measurements were collected in WSL2 and may vary with hardware and system load.
- Distribution measurements use a fixed set of 100,000 keys.
- Vnode distribution can vary depending on the generated hash positions.
- Lookup measurements include hashing and binary search, not network communication or replication.
- Construction and lookup timings are single-run measurements rather than statistically controlled benchmarks.
- These results evaluate the ring library independently, not end-to-end distributed cache performance.

---

## Phase 3 — Node Failure Detection

### Environment and Configuration

- Date: 06-11-2026
- Machine: Windows laptop (WSL2)
- OS: Ubuntu 24.04
- Python: 3.12
- Cluster: 5 nodes
- Failed node: n3
- Failure simulation: Manual process termination
- Detection mechanism: SWIM-style direct and indirect probing with gossip-based membership dissemination
- Measurement: Time between terminating n3 and the last healthy node reporting it as DEAD

### 1. Node Failure Detection Time

The experiment measured how long the cluster took to detect a failed node after its process was terminated.

The five-node cluster was started using:

```bash
./scripts/run_cluster.sh 5
```

Node n3 was manually terminated while the remaining healthy nodes continued running.

The cluster's membership state was monitored until all healthy nodes reported n3 as `DEAD`.

**Observed Results:**

| Metric | Result |
|---|---:|
| Total cluster nodes | 5 |
| Failed node | n3 |
| Failure event | Manual process termination |
| Node termination timestamp | 10:23:02.637 |
| Last DEAD report timestamp | 10:23:12.435 |
| Total detection time | 9,798 ms |
| Detection time (seconds) | 9.80 seconds |

**Detection Time Calculation:**

```text
Detection Time = Last DEAD Report - Node Termination Time

               = 10:23:12.435 - 10:23:02.637

               = 9.798 seconds

               = 9,798 ms
```

### 2. Observations

- Node n3 was manually terminated to simulate an unexpected node failure.
- The remaining healthy nodes continued operating during the failure simulation.
- The cluster detected the failed node and updated its membership state to `DEAD`.
- The last healthy node reported n3 as `DEAD` approximately 9.80 seconds after termination.
- This demonstrates that failure information propagated across the cluster through the gossip mechanism.

### 3. Conclusion

The Phase 3 failure detection experiment demonstrated that the cluster can detect an unexpected node failure and propagate the updated membership state to the remaining healthy nodes.

In the five-node cluster experiment, the last healthy node reported the failed node as `DEAD` approximately 9.80 seconds after its process was terminated.

This establishes an initial baseline for gossip-based failure detection. Further experiments can evaluate detection time across different cluster sizes, gossip intervals and failure scenarios.

### 4. Known Limitations

- Only one failure detection experiment was recorded.
- The experiment used manual process termination rather than network partition simulation.
- Measurements were collected in WSL2 and may vary with CPU load and scheduling.
- Detection time was measured until the last healthy node reported the failed node as `DEAD`.
- The result does not establish an average or worst-case detection time.
- No repeated trials or statistical analysis were performed.

---

## Phase 4 — Replication and Survival

### Environment and Configuration

The Phase 4 measurements include a single-node performance benchmark and a separate five-node failure/restart experiment.

These are different tests and should not be interpreted as one combined distributed benchmark.

### 1. Single-Node Performance After Phase 4 Changes

**Environment:**

- Date: 07-11-2026
- Machine: Windows laptop (WSL2)
- Python: 3.12
- Running node: `n1`
- Client port: 7001
- Membership observed at startup: `n1` only
- Configured replication factor: 3
- Configured write acknowledgements: 1

The benchmark was run after the Phase 4 read and replication changes.

**Default benchmark:**

```bash
python bench/bench.py
```

Results:

| Metric | Result |
|---|---:|
| Total operations | 200,000 |
| Execution time | 9.70 seconds |
| Operations/sec | 20,623 |

**100-client benchmark:**

```bash
python bench/bench.py --clients 100
```

Results:

| Metric | Result |
|---|---:|
| Total operations | 400,000 |
| Execution time | 20.04 seconds |
| Operations/sec | 19,960 |
| Concurrent clients | 100 |

### 2. Comparison Against Phase 1

| Benchmark | Phase 1 | Phase 4 | Change |
|---|---:|---:|---:|
| Python benchmark | 30,111 ops/sec | 20,623 ops/sec | -31.5% |
| Python benchmark (100 clients) | 30,729 ops/sec | 19,960 ops/sec | -35.1% |

**Observations:**

- The default benchmark decreased from approximately 30.1k to 20.6k operations per second.
- The 100-client benchmark decreased from approximately 30.7k to 20.0k operations per second.
- This is a measurable single-node throughput regression compared with the Phase 1 baseline.
- The benchmark does not establish the exact cause of the regression.

**Important benchmark limitation:**

This was a single-node run. The membership output showed only `n1`, even though the configuration specified a replication factor of three.

Therefore, this experiment does not measure:

- Three-node replication traffic.
- Internal RPC overhead across multiple nodes.
- Sloppy-quorum stand-in writes.
- Hinted handoff between different nodes.
- Distributed read fanout across multiple nodes.
- Multi-node failure handling.

A replication factor configured as three does not mean that three active nodes were present in this benchmark.

The results should be described as a single-node performance comparison after the Phase 4 code changes, not as a distributed-cluster throughput benchmark.

### 3. Five-Node Failure and Restart Experiment

The chaos test exercised a five-node cluster while one node was terminated and subsequently restarted.

Command:

```bash
python chaos/crude_kill.py
```

The script started five nodes using client ports `7101`–`7105`, terminated `n2`, and restarted it.

**Recorded output:**

```text
[  1.5s] cluster up: ['127.0.0.1:7101', '127.0.0.1:7102', '127.0.0.1:7103', '127.0.0.1:7104', '127.0.0.1:7105']
[  9.5s] KILLED n2
[ 21.5s] RESTARTED n2

=== RESULT ===
requests=237908  acked_writes=118954
failed_requests=0  client_retries=5
lost_acked_writes=0
latency p50=1.3ms  p99=3.6ms  max=35ms
PASS
```

**Results:**

| Metric | Result |
|---|---:|
| Cluster size | 5 nodes |
| Failed node | n2 |
| Requests | 237,908 |
| Acknowledged writes | 118,954 |
| Failed requests | 0 |
| Client retries | 5 |
| Lost acknowledged writes | 0 |
| p50 latency | 1.3 ms |
| p99 latency | 3.6 ms |
| Maximum latency | 35 ms |
| Test result | PASS |

### 4. Observations

- The five-node cluster started successfully.
- Node `n2` was terminated while the test workload was running.
- The remaining nodes continued processing requests.
- The test recorded five client retries.
- The script reported zero failed requests.
- The script reported zero lost acknowledged writes after the node was restarted.
- The recorded p99 latency was 3.6 ms, with a maximum latency of 35 ms.

The experiment demonstrates that the tested failure and restart scenario completed successfully according to the chaos script's acknowledged-write verification.

### 5. Interpretation and Limitations

This result is evidence for one particular failure and restart scenario. It is not proof of general durability or recovery under every failure condition.

Important limitations:

- The experiment is a single recorded run.
- It does not test simultaneous failures of multiple nodes.
- It does not test a network partition.
- It does not test loss of a stand-in before hint replay.
- It does not establish recovery of every historical key after restart.
- Storage and hints are in-memory and are not durable across process restarts.
- The result does not independently prove that every possible acknowledged write survives every possible failure sequence.

The reported zero lost acknowledged writes should be interpreted as the result of the script's verification for this run, not as a general durability guarantee.

### 6. Conclusion

The Phase 4 experiments evaluate two different aspects of the implementation.

The single-node benchmark shows a throughput regression compared with the original Phase 1 baseline. Further profiling and multi-node benchmarks are required to understand the performance impact of the new coordination and replication logic.

The five-node chaos experiment passed its acknowledged-write verification during one node failure and restart scenario, recording zero lost acknowledged writes and zero failed requests.

Together, these results provide an initial baseline for evaluating the performance and failure-handling behavior of the Phase 4 implementation.

### 7. Next Benchmarking Work

Further experiments should include:

- Single-node profiling to identify the source of the throughput regression.
- A five-node benchmark with the cluster membership verified before measurements.
- Comparison of strict preference-list writes and sloppy-quorum writes.
- Read latency measurements with different `read_fanout` values.
- Measurements with and without hedged reads.
- Hint replay timing after a node returns.
- Hint-store capacity and expiration behavior.
- Repeated node-kill and restart trials.
- Network partition and multiple-node failure scenarios.
- Recovery testing for historical keys that were not accessed during downtime.

---

## Overall Benchmark Summary

| Phase | Component | Main Result |
|---|---|---|
| Phase 1 | Single-node RESP server | ~30k operations/sec |
| Phase 2 | Consistent hashing ring | 1.48 μs average lookup |
| Phase 2 | Ring construction | 9.3 ms (10 nodes, 150 vnodes each) |
| Phase 2 | Key movement | 8.8% (10 → 11 nodes) |
| Phase 2 | Distribution | 9.4% CV (150 vnodes) |
| Phase 3 | Gossip-based failure detection | 9.80 seconds (5-node cluster) |
| Phase 4 | Single-node benchmark | 20,623 ops/sec |
| Phase 4 | Single-node benchmark (100 clients) | 19,960 ops/sec |
| Phase 4 | Five-node failure/restart test | PASS; 0 lost acknowledged writes in the recorded run |

### Overall Conclusion

The benchmarks establish initial performance and behavior baselines for the self-healing distributed cache.

- **Phase 1:** The single-node RESP server achieved approximately 30k operations per second.
- **Phase 2:** Consistent hashing reduced key movement during membership changes, with low lookup overhead and reasonably balanced key distribution.
- **Phase 3:** The five-node cluster successfully detected a terminated node and propagated the `DEAD` state to all remaining healthy nodes in approximately 9.80 seconds.
- **Phase 4:** The single-node benchmark showed a throughput regression after the new implementation changes. A separate five-node chaos experiment passed its acknowledged-write verification during one node failure and restart scenario.

These results provide a foundation for evaluating the performance, scalability and fault tolerance of subsequent cluster implementations.

All measurements should be treated as initial experimental baselines rather than statistically validated performance guarantees.