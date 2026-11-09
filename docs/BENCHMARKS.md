# SelfHeal Cache — Benchmark Results

## Benchmark Methodology and Environment

Unless otherwise stated, benchmarks were performed on a Windows laptop using WSL2.

**Common environment:**

- OS: Ubuntu 24.04 (WSL2)
- Python: 3.12
- Benchmark client: Project's Python benchmark (`bench/bench.py`)
- Client protocol: RESP
- Client port: 7001

These measurements are experimental results from the development environment. They are not statistically controlled production benchmarks.

Different phases measure different parts of the system. A single-node throughput benchmark, a ring lookup benchmark, a failure-detection experiment, and a multi-node chaos test should not be interpreted as directly comparable measurements.

---

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

Note: `redis-benchmark` was used as an alternative benchmark client against the project's RESP server on port 7001. These results do not represent the performance of a Redis server.

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

`CV = Standard Deviation / Mean`

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

## Phase 6 — Rebalancing Under Load

### Environment and Configuration

- Date: 08-11-2026
- Machine: Windows laptop (WSL2)
- OS: Ubuntu 24.04
- Python: 3.12
- Test script: `chaos/join_under_load.py`
- Initial cluster: 3 nodes
- Replication factor: 3
- Write acknowledgements: 2
- Initial preload: 20,000 keys
- Concurrent workload workers: 16
- New node: n4
- Gracefully leaving node: n2
- Join settle delay used for transfer-rate calculation: 3 seconds

The experiment evaluated whether the cluster could add a new node and gracefully remove an existing node while client writes continued.

The script started three nodes, preloaded 20,000 keys, and then launched 16 concurrent workload workers. Node n4 was added while the workload was active. After n4 became `ALIVE`, node n2 was instructed to leave gracefully.

### 1. Node Join Under Load

Node n4 was started while the existing cluster continued handling client traffic.

**Recorded output:**

```text
[  5.7s] preloaded 20000 keys
[ 10.7s] STARTED n4 (joining)
[ 24.3s] n4 ALIVE after 13.6s

node_id:n4
live_keys:69589
raw_records:69589
hints_held:0
hints_rejected:0
hints_written:0
read_repairs:0
state:alive
ring_nodes:4
keys_received:47905
keys_sent:0
keys_pushed:0
keys_cleaned:0
```

**Results:**

| Metric | Result |
|---|---:|
| Initial cluster size | 3 nodes |
| Cluster size after join | 4 nodes |
| Joining node | n4 |
| Time from join start to ALIVE | 13.6 seconds |
| Settle delay used in calculation | 3.0 seconds |
| Estimated transfer duration | 10.6 seconds |
| Keys received | 47,905 |
| Live keys on n4 at INFO snapshot | 69,589 |
| Raw records on n4 at INFO snapshot | 69,589 |
| Hints held | 0 |
| Hints rejected | 0 |
| Hints written | 0 |
| Final membership state | ALIVE |

The `live_keys` and `raw_records` values are node-wide counts at the INFO snapshot. They are not the number of keys transferred during the join. The transfer count used for the calculation is `keys_received`.

### 2. Measured Join Transfer Rate

The join duration includes the settle delay before the transfer work. To estimate the actual transfer rate, the 3-second settle delay is subtracted from the observed join duration.

Calculation:

```text
Join duration = 13.6 seconds
Settle delay  = 3.0 seconds

Estimated transfer duration = 13.6 - 3.0
                            = 10.6 seconds

Keys received = 47,905

Measured transfer rate = Keys received / Transfer duration

                       = 47,905 / 10.6

                       ≈ 4,519 keys/second
```

**Transfer-rate result:**

| Metric | Result |
|---|---:|
| Keys received | 47,905 |
| Total join duration | 13.6 seconds |
| Settle delay | 3.0 seconds |
| Estimated transfer duration | 10.6 seconds |
| Measured transfer rate | approximately 4,519 keys/sec |

**Comparison with `transfer_rate`:**

The supplied INFO output reports `keys_received`, but does not show a `transfer_rate` field. Therefore, a direct comparison against the implementation's reported `transfer_rate` cannot be made from this run's output.

The independently calculated rate of approximately 4,519 keys/sec is the measured rate for this experiment. If the implementation exposes a separate `transfer_rate` statistic, record that value in a future run and compare it against this calculation using the same transfer interval.

The expected 3–5 second join time was not achieved in this run. n4 took 13.6 seconds to become `ALIVE`, including the settle period and data transfer. This indicates that the observed join took longer than the initial expectation; further repeated measurements and profiling are needed to identify the bottleneck.

### 3. Graceful Node Leave Under Load

After n4 joined successfully, the test instructed n2 to leave the cluster gracefully.

**Recorded output:**

```text
[  29.3s] n2: CLUSTER LEAVE
reply: OK
[  81.8s] n2 handed off and left in 52.5s
```

**Results:**

| Metric | Result |
|---|---:|
| Leaving node | n2 |
| Leave command response | OK |
| Time to complete graceful leave | 52.5 seconds |
| Final state | LEFT |

The test waited for n2 to reach `LEFT` before completing the workload and verification.

### 4. Client Workload and Data Integrity

The workload continued while n4 joined and n2 left.

**Recorded final output:**

```text
=== RESULT ===
acked_writes=320042 failed_requests=0 retries=0
lost_acked_writes=0
latency p50=1.5ms p99=3.8ms max=98ms
PASS
```

**Results:**

| Metric | Result |
|---|---:|
| Acknowledged writes | 320,042 |
| Failed requests | 0 |
| Client retries | 0 |
| Lost acknowledged writes | 0 |
| p50 write latency | 1.5 ms |
| p99 write latency | 3.8 ms |
| Maximum write latency | 98 ms |
| Test result | PASS |

The test verified acknowledged keys after the join and leave operations. The script reported no lost acknowledged writes.

The latency values represent client-observed write latency during the workload, not isolated server-side processing time.

### 5. Observations

- The cluster started with three nodes and successfully added n4.
- n4 transitioned from `JOINING` to `ALIVE` in 13.6 seconds.
- The new node reported 47,905 received keys.
- The independently calculated transfer rate was approximately 4,519 keys/sec after excluding the 3-second settle delay.
- The cluster completed a graceful leave for n2 in 52.5 seconds.
- The workload recorded 320,042 acknowledged writes.
- The script reported zero failed requests and zero client retries.
- The final verification reported zero lost acknowledged writes.
- The measured p99 write latency was 3.8 ms, with a maximum of 98 ms.

### 6. Interpretation and Limitations

This experiment provides evidence that the tested join and graceful-leave operations completed while client traffic was active.

The zero failed requests and zero lost acknowledged writes are positive results for this particular run. They do not establish a general guarantee that every membership transition or failure sequence will preserve all acknowledged data.

Important limitations:

- Only one recorded join-and-leave run is available.
- The measured join time was 13.6 seconds, longer than the expected 3–5 seconds.
- The transfer rate is calculated using the reported `keys_received` and an estimated transfer duration obtained by subtracting the 3-second settle delay.
- The supplied INFO output does not include a `transfer_rate` value for direct comparison.
- The experiment does not establish average or worst-case join and leave durations.
- The test does not simulate simultaneous node failures during rebalancing.
- The test does not establish durability across process or machine restarts because storage is in-memory.
- The result is specific to the tested cluster configuration, workload, and WSL2 environment.

### 7. Conclusion

The Phase 6 chaos experiment successfully exercised node addition and graceful removal while the cluster handled concurrent writes.

The new node n4 joined in 13.6 seconds and received 47,905 keys. After excluding the 3-second settle delay, the estimated transfer rate was approximately 4,519 keys/sec.

The existing node n2 completed graceful leave in 52.5 seconds.

Across the workload, the test recorded 320,042 acknowledged writes, zero failed requests, zero retries, and zero lost acknowledged writes. The experiment therefore passed its join, leave, and acknowledged-write verification for this run.

The join duration was higher than the initial 3–5 second expectation. Further repeated tests and profiling should be used to improve and characterize rebalancing performance.

### 8. Next Benchmarking Work

Further experiments should include:

- Repeat join and leave tests to measure average and worst-case durations.
- Record the implementation's `transfer_rate` statistic alongside `keys_received`.
- Compare the reported transfer rate with the independently calculated rate.
- Test joins with different initial key counts and cluster sizes.
- Measure join duration with different replication factors.
- Measure the impact of concurrent write traffic on transfer throughput.
- Test graceful leave when one of the future owners is unavailable.
- Test forced node failure during an active join or leave.
- Verify that old owners retain data until the required future owners confirm receipt.
- Profile the transfer path to identify bottlenecks if join duration remains high.

---

## Phase 7 — TTL Consistency and Clock Skew

### Environment and Configuration

- Date: 09-11-2026
- Machine: Windows laptop (WSL2)
- OS: Ubuntu 24.04
- Python: 3.12
- Server: Single node (`n1`)
- Client port: 7001
- Benchmark client: Project's Python benchmark (`bench/bench.py`)
- Versioning: Hybrid Logical Clock (HLC)
- TTL representation: Absolute Unix timestamp in milliseconds

The Phase 7 benchmark was run after implementing HLC-based versioning, clock-skew protection, TTL consistency changes, and the associated replication and synchronization paths.

### 1. Python Benchmark — Default Configuration

Command:

```bash
python bench/bench.py
```

Results:

| Metric | Result |
|---|---:|
| Total operations | 200,000 |
| Execution time | 10.84 seconds |
| Operations/sec | 18,450 |

### 2. Python Benchmark — 100 Clients

Command:

```bash
python bench/bench.py --clients 100
```

Results:

| Metric | Result |
|---|---:|
| Total operations | 400,000 |
| Execution time | 23.78 seconds |
| Operations/sec | 16,823 |
| Concurrent clients | 100 |

### 3. Comparison Against Previous Phases

| Benchmark | Phase 1 | Phase 4 | Phase 7 |
|---|---:|---:|---:|
| Python benchmark (default) | 30,111 | 20,623 | 18,450 |
| Python benchmark (100 clients) | 30,729 | 19,960 | 16,823 |

All throughput values are in operations per second.

**Change compared with Phase 1:**

| Benchmark | Phase 1 | Phase 7 | Throughput change |
|---|---:|---:|---:|
| Default | 30,111 | 18,450 | -38.7% |
| 100 clients | 30,729 | 16,823 | -45.3% |

**Change compared with Phase 4:**

| Benchmark | Phase 4 | Phase 7 | Throughput change |
|---|---:|---:|---:|
| Default | 20,623 | 18,450 | -10.5% |
| 100 clients | 19,960 | 16,823 | -15.7% |

### 4. Observations

- The default benchmark achieved 18,450 operations per second.
- The 100-client benchmark achieved 16,823 operations per second.
- Throughput is lower than the original Phase 1 baseline.
- Throughput is also lower than the Phase 4 single-node measurements.
- The 100-client run achieved lower throughput than the default run.
- The default benchmark decreased by approximately 10.5% compared with Phase 4.
- The 100-client benchmark decreased by approximately 15.7% compared with Phase 4.

These measurements show a further single-node throughput decrease after the Phase 4 benchmark. They do not establish which individual Phase 7 feature caused the decrease.

### 5. System Resource Snapshot

A system resource check was performed during the Phase 7 investigation.

| Metric | Observed value |
|---|---:|
| System memory | 7.5 GiB |
| Memory used | 3.0 GiB |
| Memory free | 4.2 GiB |
| Memory available | 4.4 GiB |
| Swap used | 0 |
| Load average | 0.19, 0.14, 0.07 |

The resource snapshot did not indicate heavy overall system load at the time it was collected.

However, this was not a continuous resource measurement synchronized with every benchmark run. It cannot establish that CPU scheduling, background processes, or other system activity had no effect on throughput.

### 6. Interpretation and Limitations

The Phase 7 measurements are single-node benchmark results. The node was started using:

```bash
python -m node --node-id n1 --port 7001
```

This invocation used the default startup configuration rather than explicitly starting the three-node configuration from `config.example.json`.

Therefore, the benchmark should not be interpreted as a three-node replication benchmark.

It measures the observed performance of the single-node server after the Phase 7 code changes. It does not isolate the cost of:

- HLC version generation and observation.
- Clock-skew validation.
- TTL deadline handling.
- HLC propagation through replication and gossip.
- Multi-node replication traffic.
- Hint replay and read repair.
- Join synchronization or graceful handoff.

The benchmark script and startup configuration should be checked together before drawing a precise conclusion about the source of the throughput regression.

The comparison against earlier phases is useful as a development performance trend, but it is not a controlled feature-by-feature benchmark. Different startup configurations, workloads, code paths, or system conditions can affect the results.

### 7. Conclusion

The Phase 7 single-node benchmark recorded:

- 18,450 operations per second for the default benchmark.
- 16,823 operations per second with 100 concurrent clients.

Compared with Phase 4, throughput decreased by approximately 10.5% for the default benchmark and 15.7% for the 100-client benchmark.

Compared with Phase 1, throughput decreased by approximately 38.7% and 45.3%, respectively.

These results establish the current Phase 7 performance baseline after implementing HLC-based versioning and TTL consistency changes.

The measurements show a throughput regression, but further profiling and controlled comparisons are required to identify its cause. They should not be treated as proof that HLC or TTL handling alone caused the decrease.

### 8. Next Benchmarking Work

Further experiments should include:

- Inspecting the benchmark script to confirm the exact workload and operation mix.
- Verifying that Phase 1, Phase 4, and Phase 7 use equivalent startup configurations.
- Measuring CPU usage during each benchmark run.
- Running multiple benchmark trials and recording mean and standard deviation.
- Profiling HLC version generation and timestamp observation.
- Measuring the overhead of TTL handling independently.
- Comparing a single-node run with a verified three-node cluster.
- Measuring the effect of HLC propagation through replication and gossip.
- Recording latency percentiles beyond throughput.
- Repeating the benchmarks after performance optimizations.

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
| Phase 6 | Node join | n4 ALIVE in 13.6 seconds |
| Phase 6 | Join transfer | 47,905 keys; ~4,519 keys/sec estimated |
| Phase 6 | Graceful leave | n2 LEFT in 52.5 seconds |
| Phase 6 | Join/leave under load | PASS; 320,042 acknowledged writes, 0 failed requests, 0 lost acknowledged writes |
| Phase 7 | Single-node benchmark | 18,450 ops/sec |
| Phase 7 | Single-node benchmark (100 clients) | 16,823 ops/sec |

## Overall Conclusion

The benchmarks establish initial performance and behavior baselines for SelfHeal Cache.

- **Phase 1:** The single-node RESP server achieved approximately 30k operations per second.
- **Phase 2:** Consistent hashing reduced key movement during membership changes, with low lookup overhead and reasonably balanced key distribution.
- **Phase 3:** The five-node cluster detected a terminated node and propagated the `DEAD` state to all remaining healthy nodes in approximately 9.80 seconds.
- **Phase 4:** The single-node benchmark showed a throughput regression compared with Phase 1. A separate five-node chaos experiment passed its acknowledged-write verification during one node failure and restart scenario.
- **Phase 6:** The cluster completed a node join and graceful leave while handling concurrent writes. The recorded run reported zero failed requests and zero lost acknowledged writes, with an estimated join transfer rate of approximately 4,519 keys/sec.
- **Phase 7:** The single-node benchmark recorded 18,450 ops/sec by default and 16,823 ops/sec with 100 clients after implementing HLC-based versioning and TTL consistency changes.

The results demonstrate progress in the cache's distributed functionality, but they also show that single-node throughput has decreased from the initial baseline.

The next performance priority should be to identify the cause of the throughput regression using equivalent benchmark configurations, repeated trials, and profiling.

All measurements should be treated as experimental results from the tested WSL2 environment, not statistically validated performance guarantees.