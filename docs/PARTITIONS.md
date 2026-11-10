# Network Partition Experiments

## 1. Setup

- Project: SelfHeal Cache
- Test command: `python -m chaos.partition`
- Cluster: 5 nodes
- Replication factor (N): 3
- Write acknowledgements (W): 2
- Read fanout (R): 2
- Sloppy quorum: enabled
- Consistency: eventual consistency with last-write-wins (LWW)
- Storage: in-memory

Two scenarios were tested:

1. `isolate_one`: isolate one node from the remaining four.
2. `split_3_2`: divide the cluster into groups of three and two nodes.

## 2. CAP Position

SelfHeal Cache is AP-oriented with tunable-ish consistency.

During a network partition, both sides can continue accepting writes through sloppy quorum. This preserves availability but allows replicas to diverge.

After connectivity is restored, conflicting versions are reconciled using last-write-wins (LWW).

The system does not guarantee linearizability. Phase 9 quorum mode provides an opt-in stricter consistency alternative.

## 3. Measured Results

### Scenario A — Isolate one node

| Metric | Result |
|---|---:|
| Isolated node | n2 |
| Failure detection time | 7.3 s |
| Heal-to-alive time | 1.4 s |
| Failed operations | 0 |
| Lost operations | 0 |
| Retries | 27 |
| Stale reads | 5 |
| Worst p99 latency | 1,155 ms |

**Result: PASS**

The majority continued operating while n2 was isolated. The isolated node could still serve local reads, resulting in 5 stale reads.

No operations were reported lost. The cluster recovered after healing, with all nodes returning to the ALIVE state.

### Scenario B — Split 3 vs 2

| Metric | Result |
|---|---:|
| Writes acknowledged on side A | 200 |
| Writes acknowledged on side B | 200 |
| Divergent keys after healing | 200 |
| Divergent keys after read sweep | 115 |
| Divergent keys after CLUSTER SYNC | 0 |
| Final values from side B | 200 |
| Acknowledged writes silently lost | 200 |
| Failed operations | 0 |
| Retries | 11 |
| Stale reads | 62 |
| Worst p99 latency | 1,057 ms |

**Result: PASS (informational)**

Both sides successfully acknowledged all 200 writes.

Side A wrote `from-A`, followed by side B writing `from-B`. Since side B's writes were strictly later, LWW selected `from-B` for all 200 keys.

Consequently, all 200 acknowledged `from-A` writes lost during conflict resolution.

After healing:

- All 200 keys were divergent.
- A read sweep reduced divergence to 115 keys.
- Explicit `CLUSTER SYNC` reduced measured divergence to zero.
- All 200 keys ultimately contained `from-B`.

This demonstrates that successful write acknowledgements do not guarantee that a value will survive conflict resolution during a partition.

### Load and verification

| Metric | Result |
|---|---:|
| Operations | 59,245 |
| Failed operations | 0 |
| Retries | 11 |
| Stale reads | 62 |
| Worst p99 latency | 1,057 ms |
| Verified keys | 4,608 |
| Incorrect values | 0 |

## 4. Where Consistency Is Lost

The experiments demonstrate several consistency limitations.

1. **Acknowledged writes can lose:** During a 3-vs-2 split, both sides acknowledged 200 writes. LWW subsequently discarded all 200 earlier writes from side A.

2. **Stale reads from isolated nodes:** An isolated node can continue serving local data even when that data is outdated or missing.

3. **Sloppy quorum:** Writes can be acknowledged by temporary stand-in nodes. Such acknowledgements do not mean every intended replica has received the value.

4. **Read fanout limitations:** Reads only query the configured read fanout. Replicas outside that fanout can remain divergent until another mechanism touches them.

5. **In-memory hinted handoff:** Hints are not durable. If a node holding hints crashes before replay, those hints can be lost.

## 5. Healing and Convergence

The isolated-node experiment measured 1.4 seconds from healing to all nodes reporting ALIVE.

The 3-vs-2 experiment showed three stages of convergence:

| Stage | Divergent keys |
|---|---:|
| Immediately after healing | 200 |
| After read sweep | 115 |
| After explicit CLUSTER SYNC | 0 |

Read repair alone did not fully reconcile all replicas because reads only contacted the configured read fanout.

Explicit cluster synchronization completed the reconciliation for this experiment.

This is an important distinction: restoring network connectivity does not automatically guarantee that every replica has identical data.

## 6. Limitations and Future Work

The current implementation does not proactively compare every replica's dataset after a partition.

A replica that is never read or explicitly synchronized can remain divergent.

**Incremental Merkle-tree-based anti-entropy** is a natural future improvement. It would allow nodes to compare dataset summaries and identify missing or divergent keys without scanning the entire keyspace on every synchronization cycle.

The following scenarios were not tested:

- Asymmetric partitions (one side can reach the other, but not vice versa).
- Flapping network links.
- Network partitions during node join.
- Network partitions during node leave.

## 7. Conclusion

Both partition scenarios passed.

The experiments verified that SelfHeal Cache continues accepting operations during network partitions, supports recovery after connectivity is restored, and resolves conflicting writes using LWW.

However, availability comes with measurable consistency trade-offs. In particular, the 3-vs-2 experiment demonstrated that 200 acknowledged writes can be superseded and silently lost during reconciliation.

The experiment also demonstrated that read repair alone is insufficient for complete convergence in all cases, while explicit cluster synchronization reconciled all measured divergent keys.

These results establish a concrete baseline for future anti-entropy improvements.
