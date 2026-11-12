# SelfHeal Cache — Design Document

## 1. Goals and Non-Goals

SelfHeal Cache is a leaderless, Dynamo-style distributed in-memory key-value store built from scratch in Python using `asyncio`.

### Goals

1. Provide a Redis-compatible RESP interface for client operations.
2. Allow any node to coordinate requests without a permanent leader.
3. Distribute keys using consistent hashing and virtual nodes.
4. Replicate data across distinct physical nodes.
5. Provide eventual consistency with Last-Write-Wins (LWW) conflict resolution by default.
6. Detect node failures and recover through failover, hinted handoff and synchronization.
7. Support absolute-deadline TTL expiration, tombstones and safe garbage collection.
8. Support dynamic membership, historical data transfer and ownership cleanup.
9. Provide optional quorum consistency and memory-bounded LRU/LFU eviction.

### Non-Goals

- Disk persistence or crash-safe recovery.
- Transactions or general exactly-once execution.
- Linearizability or consensus-based coordination.
- Automatic preservation of every concurrent write.
- WAN-scale membership and replication.
- Authentication, authorization or encrypted communication.

The project focuses on implementing and understanding distributed-systems mechanisms, not replacing a production database.

## 2. Architecture Overview

See [ARCHITECTURE.md](ARCHITECTURE.md) for architecture diagrams and request lifecycles.

The cluster follows a leaderless, Dynamo-style architecture. Every node can receive client requests and coordinate reads and writes.

Each node exposes two separate network interfaces:

| Interface | Purpose |
|---|---|
| Client interface | RESP protocol for Redis-compatible commands |
| Internal interface | Node-to-node communication for replication, membership, failure detection and synchronization |

The internal RPC protocol uses length-prefixed JSON messages. It is separate from the client-facing RESP protocol.

A typical write follows this path:

1. A client sends a command to any node.
2. That node becomes the coordinator for the request.
3. The coordinator calculates the key's preference list using the ring.
4. It creates a version and expiration deadline where applicable.
5. It sends the record to the intended replicas.
6. Replicas apply the record using version comparison.
7. The coordinator collects acknowledgements according to the configured consistency mode.
8. The client receives the result.

The read path consults replicas, compares available versions and performs read repair when required.

Membership gossip and failure detection operate independently of normal client requests. Rebalancing handles historical data movement when ownership changes.

## 3. Data Model: Records, Versions, Tombstones and TTL

### 3.1 Record

A stored record represents the latest known state of a key.

Its logical fields are:

- `key`: The key being stored.
- `value`: The stored value, or `None` for a tombstone.
- `version`: An HLC integer.
- `expire_at`: An absolute Unix timestamp in milliseconds, or no deadline.

The complete LWW ordering uses:

`(hlc_timestamp, node_id)`

The node ID provides a deterministic tie-breaker when two versions have the same HLC timestamp.

### 3.2 Versioning

Every new write receives a version from the coordinator's Hybrid Logical Clock.

Replicas compare versions lexicographically and retain the newer record.

This allows replicas to converge regardless of message arrival order.

LWW does not preserve concurrent values. When two coordinators write different values concurrently, one version wins and the other is discarded.

### 3.3 Tombstones

Deletion is represented by a versioned record with `value = None`.

A tombstone is necessary because a replica may miss a deletion while unavailable. Without retained deletion metadata, that replica could later reintroduce an older value.

Tombstones therefore propagate through replication, handoff, repair and synchronization like ordinary records.

### 3.4 TTL and Expiration

TTL is represented using an absolute expiration deadline:

`expire_at = current_wall_clock_ms + ttl_ms`

The coordinator's command handler creates this deadline. Replication, read repair and rebalancing preserve the original timestamp.

A replica must never restart a relative TTL when receiving an existing record.

After expiration, the value is no longer visible to reads. Its expiration metadata is retained during the tombstone grace period.

The record becomes eligible for garbage collection only after:

`expire_at + tombstone_grace_ms`

Expired values transferred during rebalancing are sent as bare tombstones so that expired data cannot become visible again.

## 4. Placement: Consistent Hashing and Range Ownership

### 4.1 Hash Ring

The cluster uses consistent hashing to distribute keys.

Each physical node is represented by multiple virtual nodes (vnodes) on a circular hash space.

A key is hashed to a position, and ownership starts at the next token clockwise.

The implementation uses MD5 to generate deterministic 64-bit ring positions. MD5 is used as a non-security hash function.

The configured vnode count is 150 per physical node.

### 4.2 Vnode Count and Distribution

The vnode count was selected using a distribution benchmark.

For the measured configuration of 10 nodes, 100,000 keys and 150 vnodes per node:

| Measurement | Result |
|---|---:|
| Coefficient of variation | ~9.4% |
| Maximum-to-mean ownership ratio | ~1.14 |
| Ring build time | ~9.3 ms |
| Average lookup time | ~1.48 μs |
| Key movement when adding node 11 | ~8.8% |
| Theoretical movement | ~9.1% |

These are measurements from the tested environment, not universal performance guarantees.

### 4.3 Token Naming and Node Identity

Virtual token names use:

`{node_id}#{i}`

Examples:

- `n1#0`
- `n1#1`
- `n2#0`

Node IDs must be stable and unique across restarts.

A restarted node must reuse its original ID. A new ID represents a different ring member and causes ownership to change.

### 4.4 Ring Operations and Complexity

Let `V` represent the total number of virtual nodes.

- Ring rebuilding: `O(V log V)`.
- Key lookup using binary search: `O(log V)`.

The ring is rebuilt when ring-eligible membership changes, not for every client request.

### 4.5 Preference List

A key's preference list contains distinct physical nodes in clockwise ring order.

Because a physical node owns multiple virtual tokens, the lookup skips duplicate node IDs.

This ensures that a replication factor of three means three distinct physical nodes rather than three virtual tokens belonging to fewer machines.

The first node in the preference list is the primary owner.

### 4.6 Ring Walk

The ring walk continues clockwise beyond the preference list and yields each physical node at most once.

It is used to:

- Find healthy stand-ins for unavailable replicas.
- Find alternative sources during synchronization.
- Identify additional nodes during ownership transitions.

### 4.7 Range Diff

The ring-diff implementation compares ownership between two ring configurations.

Instead of checking every key individually, it calculates contiguous hash ranges where the preference lists differ.

Each range records:

- Its lower and upper hash boundaries.
- The old preference list.
- The new preference list.
- Nodes that gained ownership.
- Nodes that lost ownership.

The ranges are sorted, disjoint and bounded by the hash-space limits.

This allows rebalancing to transfer only data affected by an ownership change.

### 4.8 Membership and Ring Separation

A node becoming `SUSPECT` or `DEAD` does not automatically change ring ownership.

Unavailable nodes are skipped during routing, but their ring positions remain intact.

This prevents temporary failures and node flapping from repeatedly reshuffling keys.

A newly discovered node enters the ring through membership discovery. A node reaching `LEFT` is excluded from the active ring.

## 5. Membership and Failure Detection

### 5.1 Membership Table

Each node maintains its own membership table.

An entry contains:

`(node_id, host, port, rpc_port, state, incarnation)`

Node IDs are stable across restarts.

Membership is maintained independently by every node and exchanged through gossip.

### 5.2 Membership States

| State | Rank | Meaning |
|---|---:|---|
| `JOINING` | 0 | Discovered node undergoing activation and synchronization |
| `ALIVE` | 1 | Healthy and available node |
| `SUSPECT` | 2 | Node suspected of being unavailable |
| `LEAVING` | 3 | Node performing graceful departure |
| `DEAD` | 4 | Node declared unavailable |
| `LEFT` | 5 | Node that has completed departure or been explicitly forgotten |

State precedence:

`JOINING < ALIVE < SUSPECT < LEAVING < DEAD < LEFT`

`LEFT` is terminal for the membership entry. A stale update must not accidentally bring a deliberately removed node back into the ring.

### 5.3 Membership Merge Order

Membership entries are merged using:

`(incarnation, state_rank)`

The entry with the higher incarnation wins. If incarnations are equal, the higher-ranked state wins.

Examples:

| Local entry | Remote entry | Winner |
|---|---|---|
| `(10, ALIVE)` | `(11, SUSPECT)` | Remote |
| `(10, ALIVE)` | `(10, SUSPECT)` | Remote |
| `(10, DEAD)` | `(11, ALIVE)` | Remote |
| `(10, DEAD)` | `(10, ALIVE)` | Local |
| `(10, DEAD)` | `(10, LEFT)` | Remote |

The merge operation is deterministic, commutative, associative and idempotent under this ordering.

### 5.4 Incarnation and Refutation

Incarnation identifies a node's membership generation.

When a node receives a suspicion about itself, it refutes the accusation by increasing its own incarnation and advertising its `ALIVE` state.

A higher incarnation takes precedence over stale accusations.

Incarnations are initialized using the boot-time Unix timestamp in milliseconds. They are not persisted across restarts.

A node speaks for itself: remote claims about its own membership entry cannot overwrite its local entry. Only its own refutation can advance that entry.

This prevents another node from permanently declaring a healthy node dead through a stale gossip update.

### 5.5 SWIM-Style Failure Detection

Failure detection uses direct and indirect probing.

The process is:

1. A node selects a peer and sends a direct ping.
2. If the direct probe fails, it asks two helper nodes to probe the target indirectly.
3. If all attempts fail, the target becomes `SUSPECT`.
4. The target has an opportunity to refute the suspicion by increasing its incarnation.
5. If the suspicion timeout expires without refutation, the target becomes `DEAD`.
6. Membership changes propagate through gossip.

A node is not immediately declared dead after a single failed network request.

### 5.6 Gossip

Nodes exchange their membership tables using full-table push-pull gossip.

Gossip also exchanges the latest known HLC timestamp.

This allows membership changes and logical-clock progress to propagate between nodes.

Full-table gossip is intentionally simple and suitable for small clusters. It is not designed for very large membership sets.

### 5.7 Failure Detection Timings

The configured timing targets are:

| Parameter | Value |
|---|---:|
| Probe interval | 1,000 ms |
| Suspicion timeout | 5,000 ms |
| Indirect probe helpers | 2 |
| Expected detection time | ~6–8 seconds |
| Operational target | Under 10 seconds |

These values are targets under normal conditions, not hard upper bounds.

Network delay, scheduling and gossip propagation can affect the actual detection time.

A normal process stop does not automatically announce graceful departure. Other nodes detect it through failure detection. Graceful departure uses `LEAVING` and `LEFT`.

## 6. Replication: Write Path, Read Path and Recovery

### 6.1 Replication Factor

Each key has a preference list of up to `N` distinct physical nodes.

The default project configuration uses three replicas.

Any node can coordinate a write or read for a key.

### 6.2 Write Path

1. The coordinator calculates the preference list.
2. It generates an HLC version.
3. It creates the record and its expiration deadline, if applicable.
4. It sends the record to preference-list replicas.
5. Each replica observes the incoming HLC timestamp and applies the record if its version is newer.
6. The coordinator collects acknowledgements.
7. The request succeeds or fails according to the configured write acknowledgement requirement.

The coordinator is not a permanent primary. It only coordinates that individual request.

### 6.3 Last-Write-Wins

Replicas compare:

`(hlc_timestamp, node_id)`

The higher version wins.

This makes record application idempotent with respect to receiving the same version repeatedly and allows replicas to converge through replication and repair.

Concurrent writes are not merged. LWW chooses one winner.

### 6.4 Read Path

The coordinator obtains the key's preference list and consults replicas according to `read_fanout`.

The default read fanout is two.

When multiple responses arrive, the coordinator selects the newest version using LWW ordering.

A read that receives only one successful response cannot compare it against replicas it did not consult.

### 6.5 Hedged Reads

A slow replica can delay a read even when another replica is healthy.

Hedging sends another replica request when the first request remains pending beyond `hedge_ms`.

The default hedge delay is 50 ms.

Hedging reduces waiting time by allowing another replica to respond without waiting indefinitely for the first.

Hedging is a latency optimization, not a guarantee that the returned value is globally newest.

### 6.6 Read Repair

When a read discovers that a consulted replica has an older or missing record, the coordinator schedules repair using the newest record it found.

Read repair:

- Updates stale replicas.
- Restores missing records on consulted replicas.
- Helps frequently accessed keys converge.

Read repair is demand-driven. It cannot guarantee recovery of keys that are never read and does not replace full historical synchronization.

Repair targets the relevant preference-list nodes, not arbitrary temporary read candidates.

### 6.7 Sloppy Quorum

A preference-list replica may be unreachable before failure detection marks it `DEAD`.

Sloppy quorum allows a healthy stand-in node to accept the write on behalf of the unavailable owner.

The coordinator:

1. Attempts the intended preference-list replicas.
2. Skips known-dead nodes.
3. If a direct write fails, walks clockwise beyond the preference list.
4. Selects a healthy stand-in.
5. Stores a hint containing the intended owner and record.
6. Counts the stored hint toward the configured write acknowledgement requirement in eventual mode.

A shared iterator prevents concurrent failed write slots from selecting the same stand-in during one replication operation.

Sloppy quorum improves availability but can temporarily make a write invisible to a read that consults only the original preference list.

### 6.8 Hinted Handoff

A hint represents a record that must eventually reach an unavailable preference-list owner.

It contains:

- Intended owner ID.
- Key.
- Versioned record.

When the intended owner returns, the stand-in replays the hint.

The owner applies the record using normal version comparison, so an older hint cannot overwrite a newer local version.

Hints are stored in memory and are not durable.

The hint store is bounded. When full, new hints are rejected rather than silently evicting existing hints. A write cannot receive acknowledgement through a stand-in if the required hint cannot be stored.

### 6.9 Hint Lifetime and Tombstone Safety

The configuration must satisfy:

`hint_ttl_ms <= tombstone_grace_ms`

The tombstone grace period must also account for clock skew:

`tombstone_grace_ms >= 2 * max_clock_skew_ms`

It must be long enough to protect against expected replica downtime.

A replica returning after a downtime longer than the supported grace period must be wiped and fully resynchronized before rejoining as an owner.

### 6.10 Recovery Mechanisms

| Mechanism | Purpose | Limitation |
|---|---|---|
| Read failover | Read from another reachable replica | Cannot recover data absent from all reachable replicas |
| Write failover | Use a healthy stand-in for an unavailable owner | Depends on stand-in availability and hint capacity |
| Hinted handoff | Deliver writes made while an owner was unavailable | Hints are in-memory and can be lost |
| Read repair | Repair stale or missing records discovered during reads | Only repairs keys that are read |
| `CLUSTER SYNC` | Perform historical catch-up and restore ownership copies | Requires available source replicas |

A restarted node loses its local records and hints. It must perform historical catch-up rather than being assumed fully synchronized simply because it has returned to `ALIVE`.

### 6.11 Client Retries

The client is configured with a bounded retry count.

A request can time out even after the server has processed it. Therefore, the client may not always know whether an operation committed.

Repeating ordinary `SET` or `DEL` operations can produce another version, but this does not provide general exactly-once execution.

Retrying a TTL-bearing write can also calculate a new expiration deadline.

Conditional and read-modify-write operations require additional care because they are not atomic across the cluster.

## 7. Rebalancing: Join, Leave and Cleanup

Rebalancing moves historical records when ring ownership changes.

It supports:

- New node joins.
- Restarted-node recovery.
- Graceful departure.
- Ownership cleanup.
- Administrative removal followed by synchronization.

### 7.1 Join: Pull-Based Synchronization

A joining node pulls the records belonging to its new ownership ranges from existing live replicas.

The process:

1. The new node enters `JOINING`.
2. Membership changes propagate through gossip.
3. The node waits for `rebalance_settle_ms`, approximately three gossip rounds.
4. The ownership transition is calculated using the old and new rings.
5. The joiner identifies the ranges it needs.
6. It selects a live source replica and requests records.
7. Records are streamed in batches and applied using normal version comparison.
8. If a source fails, the joiner tries another candidate.
9. The joining node participates in writes so that new updates can reach it during historical transfer.
10. It remains demoted in normal read preference until synchronization completes.
11. When synchronization finishes, it becomes `ALIVE`.

During the transition, reads are widened to include the joining node and displaced old replicas.

Read repair remains restricted to preference-list nodes.

### 7.2 Settle Window

The configured `rebalance_settle_ms` gives membership gossip time to disseminate a new node's presence before ownership transitions are acted upon.

The intended delay is approximately three gossip rounds.

This relies on the assumption:

`No coordinator is partitioned from gossip for longer than rebalance_settle_ms`

If a coordinator remains partitioned beyond this period, it may continue writing without including the joiner.

The settle window reduces stale membership views but is not a consensus protocol.

### 7.3 Incomplete Join

A joiner retries unavailable sources with backoff.

If synchronization cannot finish within `join_max_wait_ms`, the node may transition to `ALIVE` with incomplete historical data.

This favors availability over indefinite blocking.

Incomplete synchronization is logged as an error. Read repair and a subsequent `CLUSTER SYNC` can repair missing records when a sufficiently new copy survives elsewhere.

`ALIVE` does not necessarily mean that every historical record has been synchronized.

### 7.4 Graceful Leave: Push-Based Handoff

A graceful departure uses the `LEAVING` state.

The process:

1. The node receives `CLUSTER LEAVE`.
2. It transitions to `LEAVING`.
3. The future ring is calculated as `ring_after`, excluding the leaving node.
4. Writes are sent to both normal preference-list owners and future owners.
5. Additional writes to future owners are best-effort and do not count toward `W`.
6. The leaving node scans its local records.
7. It transfers records to the future owners.
8. Future owners apply records using normal version comparison.
9. The node completes the handoff and transitions to `LEFT`.
10. `LEFT` propagates through gossip and the node leaves the active ring.

If handoff fails or times out, the node reverts to `ALIVE` with a bumped incarnation.

The node must not be considered successfully departed while required handoff work remains unresolved.

### 7.5 Cleanup and Ownership Confirmation

After ownership changes, old owners may retain records that no longer belong to them.

Cleanup must not delete a record merely because a transfer was attempted.

The old owner:

1. Identifies records it no longer owns.
2. Determines the current owners for each key.
3. Checks that every required owner has a version greater than or equal to its local version.
4. Deletes the obsolete copy only after all confirmations succeed.
5. Retains the record and retries if any owner is unreachable or has an older version.

Cleanup runs at a controlled interval using `cleanup_interval_ms`.

Expired records are transferred as bare tombstones.

### 7.6 Administrative Commands

| Command | Purpose |
|---|---|
| `CLUSTER NODES` | Inspect known nodes, states, addresses and incarnations |
| `CLUSTER LEAVE` | Start graceful departure of the local node |
| `CLUSTER FORGET <id>` | Explicitly remove an eligible permanently dead or leaving node |
| `CLUSTER SYNC` | Synchronize historical data and restore replica ownership |

`CLUSTER FORGET` changes membership and ring ownership. It does not automatically restore the desired number of copies.

Run `CLUSTER SYNC` on surviving nodes after a permanent removal.

### 7.7 Rebalancing Failure Cases

| Situation | Behavior |
|---|---|
| Source fails during join | Joiner tries another source |
| All sources unavailable | Retries until the configured wait limit |
| Joiner crashes | Restart begins a fresh synchronization session |
| Leaver crashes mid-drain | Peers retain the transition state; restart it or use administrative recovery |
| Leave handoff fails | Node returns to `ALIVE` with a newer incarnation |
| Owner unreachable during cleanup | Record is retained |
| Permanently dead node forgotten | Survivors require `CLUSTER SYNC` |
| Coordinator misses a join beyond settle window | Some join writes may be missed; later repair requires a surviving newer copy |

Synchronization sessions are in-memory. A restarted joiner starts a fresh join.

## 8. Time: Two Time Axes, HLC and Garbage Collection

The system uses two different notions of time. They must never be mixed.

### 8.1 Physical Wall-Clock Time

Physical time is measured in Unix milliseconds.

It is used for:

- TTL expiration deadlines.
- Tombstone garbage-collection deadlines.
- Membership incarnation initialization.
- Clock-skew validation.

An expiration deadline is an absolute physical timestamp.

### 8.2 Hybrid Logical Clock

Record versions use HLC integers, not raw wall-clock milliseconds.

The timestamp layout is:

`hlc_timestamp = (physical_time_ms << 16) | logical_counter`

The upper bits contain physical milliseconds. The lower 16 bits contain a logical counter.

`hlc_ms()` extracts the physical millisecond component.

The complete version is:

`(hlc_timestamp, node_id)`

The node ID is a deterministic tie-breaker.

### 8.3 HLC Generation

Conceptually, the next timestamp advances beyond the previous local timestamp while incorporating current physical time:

`next_timestamp = max(current_hlc, previous_hlc + 1)`

This ensures that a coordinator's generated timestamps remain monotonic even when multiple writes occur during the same physical millisecond.

### 8.4 Observing Remote HLC Timestamps

When a node receives a remote record or gossip timestamp:

1. It extracts the remote HLC physical component.
2. It compares it against its local HLC and wall clock.
3. It accepts and observes the timestamp if it is within the configured drift allowance.
4. It rejects timestamps too far ahead of local physical time.
5. Rejections increment `clock_drift_rejects`.

The default configuration is:

`max_clock_skew_ms = 5000`

Rejecting a remote HLC timestamp does not necessarily reject the accompanying membership gossip.

HLC observation communicates logical-clock progress, not proof that all records from the sender have been received.

### 8.5 Clock-Skew Experiment

The implementation supports deterministic clock tests and manual multi-node experiments.

A representative three-node setup uses:

- `n1`: Normal system clock.
- `n2`: Normal system clock.
- `n3`: Clock shifted approximately two seconds ahead.
- `max_clock_skew_ms`: 5,000.

A two-second future offset should be accepted within the configured allowance. A larger offset can exercise the rejection path.

Useful metrics:

| Metric | Meaning |
|---|---|
| `clock_drift_rejects` | Remote HLC timestamps rejected for excessive drift |
| `hlc_lead_ms` | Difference between the latest known HLC physical component and local wall time |

`hlc_lead_ms` is not a direct measurement of clock skew. A negative value can simply mean that the latest known HLC event is older than the current wall clock.

### 8.6 TTL and HLC Are Independent

HLC improves version ordering but does not make TTL independent of physical clocks.

A record's expiration deadline is created once using wall-clock milliseconds and preserved across all replicas and transfers.

The two values have different purposes:

| Value | Purpose |
|---|---|
| HLC integer | Ordering record versions |
| `expire_at` | Determining when a record expires |

Wall-clock milliseconds must never be compared directly against a packed HLC integer.

### 8.7 Tombstone Garbage Collection

Garbage collection must retain deletion metadata long enough to prevent stale values from being resurrected.

The configuration must satisfy:

`hint_ttl_ms <= tombstone_grace_ms`

and:

`tombstone_grace_ms >= 2 * max_clock_skew_ms`

The grace period must also exceed the supported hint lifetime and account for expected replica downtime.

A node returning after a downtime beyond the supported grace period must be wiped and fully resynchronized before becoming an owner.

## 9. Eviction: Pinning, Soft Cap and Accounting

Each node can enforce an approximate local store-memory budget using LRU or LFU eviction.

A memory limit of zero means unlimited store memory.

### 9.1 Memory Accounting

The accounting charge per record is:

`len(key) + len(value) + OVERHEAD`

The calibrated overhead is 261 bytes per record.

A `tracemalloc` calibration using 100,000 records measured:

| Value size | Traced bytes/record | Accounted bytes/record |
|---|---:|---:|
| 10 bytes | ~283 | 282 |
| 100 bytes | ~372 | 372 |
| 1,000 bytes | ~1,272 | 1,272 |

These measurements are specific to the tested record shapes.

`tracemalloc` is not process RSS and does not account for every source of container or process memory. The configured cap is an approximate store budget, not a hard process-memory limit.

### 9.2 LRU

LRU evicts the least recently used eligible key.

An ordered structure maintains recency, allowing access/touch and removal operations in constant time.

### 9.3 LFU

LFU evicts the least frequently used eligible key.

- Frequency counters saturate at 255.
- Ties fall back to recency.
- Periodic aging halves counters so historical popularity does not remain permanent.

The eviction simulation measured higher LFU hit rates than LRU at all five tested cache capacities on both Zipf and Zipf-plus-scans workloads.

These results are workload-specific, not proof that LFU is universally better.

### 9.4 Pinning and Tombstones

Only live values are ordinary eviction candidates.

The following records are pinned:

- Tombstones.
- Expired records still inside their grace period.

This prevents eviction from removing deletion metadata and enabling stale-data resurrection.

If memory pressure requires reclaiming an expired value's payload, the payload can be dropped while retaining its version as a bare tombstone.

Pinned records can therefore keep the store above its configured cap.

This is intentional: data-safety metadata takes priority over the soft memory limit.

### 9.5 Eviction Behavior

- Eviction is local to each node.
- Eviction does not create a tombstone.
- Eviction does not trigger replication or hinted handoff.
- Another replica may still serve the value.
- Read repair may refill an evicted copy, subject to the local cap.
- Hints are stored separately and are not included in the record-store cap.
- Only client-serving reads refresh recency.
- Internal streaming, handoff and repair traffic do not pollute eviction statistics.

The `evict_blocked` metric records situations where pinned records prevent the store from meeting its configured cap.

## 10. Consistency Modes: Eventual vs Quorum

Consistency is configured cluster-wide.

`eventual` is the default mode. `quorum` requires strict preference-list acknowledgements.

For replication factor `N`, read quorum `R` and write acknowledgements `W`, startup validation requires:

`R + W > N`

The Phase 9 example uses:

`N = 3, R = 2, W = 2`

### 10.1 Consistency Comparison

| Behavior | Eventual (default) | Quorum |
|---|---|---|
| Write acknowledgement | W acknowledgements; stand-ins and hints can count | W acknowledgements from preference-list replicas only |
| Read success | Any configured read-fanout response can be sufficient, with at least one successful response | Requires R replica responses |
| Stale replica found | Repair scheduled in the background | Stale replicas in the read response are repaired before replying |
| Partition behavior | Reachable replicas can continue accepting writes and diverge | Operations fail when the required per-key quorum is unavailable |
| Main guarantee | Eventual convergence through repair and synchronization, with possible stale reads | Overlapping read/write quorums provide conditional read-after-write visibility |

### 10.2 Quorum Overlap

For a key with `N` replicas:

`R + W > N`

means that the read and write acknowledgement sets must overlap when both are drawn from the same preference list.

For example:

`N = 3, R = 2, W = 2`

A successful write is acknowledged by at least two replicas. A successful read consults at least two replicas.

At least one replica must belong to both sets.

Because records are versioned, that overlapping replica provides the acknowledged write or a newer version to the read.

### 10.3 Important Qualification: Sloppy Quorum

The overlap argument applies only when acknowledgement sets are drawn from the same preference list.

In eventual mode, a stand-in outside the preference list can store a hint and satisfy a write slot.

A later read over the original preference list is not guaranteed to consult that stand-in.

Hint replay and read repair help close this temporary visibility gap.

Quorum mode therefore does not count stand-ins or hints toward its strict write acknowledgement requirement.

### 10.4 Quorum Limitations

Quorum mode is not linearizable.

It does not provide:

- Compare-and-set.
- Atomic cluster-wide `SET NX` or `SET XX`.
- Atomic `EXPIRE`, `PEXPIRE` or `PERSIST`.
- Transactions.
- Rollback of partially applied writes.

A write may reach some replicas and still fail at the client because fewer than W acknowledgements arrive.

Those partial writes are not rolled back and can later participate in LWW conflict resolution.

A failed write can therefore resurface after a partition heals.

Synchronous repair during quorum reads narrows temporary disagreement but does not replace consensus.

### 10.5 Per-Key Partition Availability

Quorum availability is determined by the placement of each key's replicas, not simply by whether a node belongs to the majority or minority side of a partition.

In a 3-vs-2 node partition with three replicas per key, a key's replicas may be distributed 3+0 or 2+1 across the two sides.

Only the side holding the required number of replicas can serve that key in quorum mode.

It is therefore inaccurate to claim that the entire minority side always refuses every key or that the majority side can serve every key.

### 10.6 CAP Positioning

- Eventual mode is availability-oriented (AP-style): reachable replicas can accept operations during partitions, with divergence resolved later.
- Quorum mode is CP-leaning on a per-key basis: operations that cannot reach the required quorum fail rather than using sloppy stand-ins.

Neither mode makes the system a linearizable database.

### 10.7 Eviction and Quorum

Eviction can remove a replica copy after a successful write.

This invalidates the normal quorum-overlap freshness argument because the overlapping copy may no longer exist.

The documented quorum read-after-write condition therefore assumes:

- Stable ring ownership.
- Eviction disabled.
- Strict preference-list quorums.
- The configured overlap condition.

## 11. Verification: Oracle and Test Scenarios

Verification distinguishes implementation defects from expected eventual-consistency behavior.

The test suite combines:

- Unit tests.
- In-process integration clusters.
- Injected clocks and skewed-clock tests.
- Docker-based multi-node chaos testing.
- Load generation and correctness verification.

### 11.1 Oracle Design

Each load-generator worker owns a separate keyspace.

This prevents independent workers from racing to write the same keys and makes expected values unambiguous.

The oracle tracks operations whose outcomes are known.

- A successful write acknowledgement updates the expected value.
- A definitive failed request does not become an acknowledged expected write.
- A request with an ambiguous outcome is marked `UNCERTAIN`.

`UNCERTAIN` means the server may have processed the operation even though the client did not receive a definite response.

Such requests must not be classified as definitely committed or definitely failed.

The oracle excludes uncertain outcomes from assertions requiring a known expected value.

It does not claim exactly-once execution.

### 11.2 TTL-Aware Verification

The oracle uses a configurable TTL safety margin.

For deadline `expire_at` and margin `TTL_MARGIN_MS`:

| Observation | Verification |
|---|---|
| Before `expire_at - TTL_MARGIN_MS` | Value must still be live |
| Between `expire_at - TTL_MARGIN_MS` and `expire_at + TTL_MARGIN_MS` | Boundary is ambiguous |
| After `expire_at + TTL_MARGIN_MS` | Value must no longer be visible |

The margin accounts for scheduling and clock uncertainty.

It is a test tolerance, not an extension of the actual TTL.

### 11.3 Strict Scenarios

Strict scenarios have deterministic correctness expectations under stated assumptions.

They should fail when:

- A required client operation fails.
- A known acknowledged value is incorrectly lost.
- A known expected value is wrong outside the TTL boundary window.
- Required recovery or replica-count conditions are not satisfied.

`isolate_one` is a strict scenario.

It isolates one node while the remaining cluster continues serving traffic, heals the network and verifies recovery.

Its correctness checks include:

- No failed client operations in the measured run.
- No known acknowledged writes lost.
- Successful final verification.

Retries, temporary stale reads and latency increases are recorded separately.

### 11.4 Informational Scenarios

Informational scenarios demonstrate expected consistency trade-offs rather than behavior the implementation promises to prevent.

`split_3_2` is informational.

Both partition sides may accept writes using sloppy quorum and stand-in nodes. When communication returns, LWW chooses one winning version for conflicting keys.

The experiment measures:

- Replica divergence.
- Read-repair progress.
- Explicit synchronization.
- Final conflict-resolution outcomes.

It does not promise that every acknowledged concurrent write on both sides survives.

### 11.5 Stale Reads vs Lost Acknowledged Writes

These measurements must never be combined.

| Measurement | Meaning |
|---|---|
| `failed` | Client operations that returned failure |
| `retries` | Client retry attempts |
| `stale_reads` | Reads that observed an older value than the oracle or another known replica |
| `UNCERTAIN` | Operations with unknown commit outcome |
| `lost` | Acknowledged writes absent from the final evaluated state |
| `divergent` | Keys whose replicas disagree at the observation point |
| `wrong` | Values that violate a known oracle expectation outside the TTL boundary |
| `p99` | 99th-percentile latency for the stated measurement window |

A stale read can be temporary and repaired later.

A lost acknowledged write is a value that did not survive the final convergence procedure.

A failed request is not automatically a lost acknowledged write.

### 11.6 Recorded Partition Experiments

The recorded `isolate_one` experiment used five nodes and isolated `n2`.

| Measurement | Result |
|---|---:|
| Failure detection | 7.3 seconds |
| Heal-to-ALIVE | 1.4 seconds |
| Failed requests | 0 |
| Retries | 27 |
| Stale reads | 5 |

These are measurements from that execution, not universal guarantees.

The informational `split_3_2` experiment produced:

| Stage | Divergent keys |
|---|---:|
| After healing | 200 |
| After read sweep | 115 |
| After `CLUSTER SYNC` | 0 |

Each side acknowledged 200 writes to the same test keys. The final value was `from-B` for all 200 keys, with 200 acknowledged writes from the losing side reported as silently lost through LWW conflict resolution.

The combined run recorded:

| Measurement | Result |
|---|---:|
| Operations | 59,245 |
| Failed requests | 0 |
| Retries | 11 |
| Stale reads | 62 |
| Worst observed interval p99 | 1,057 ms |
| Verified keys | 4,608 |
| Incorrect final values | 0 |

These results demonstrate convergence in the tested experiment. They do not prove that every possible partition or concurrent-write pattern will converge without loss.

## 12. Invariants

The following invariants define the safety rules that the implementation and tests must preserve.

1. **Ring ownership:** The ring changes only on first sight of a node or when a node reaches `LEFT`. Liveness changes such as `SUSPECT` and `DEAD` never independently change ownership.

2. **TTL creation:** `expire_at` is created only by the coordinator's command handler. Replication, repair and rebalancing preserve the original deadline.

3. **Version separation:** Record versions are HLC integers. Wall-clock milliseconds must never be mixed directly with packed HLC values.

4. **Hint and tombstone retention:**
   `hint_ttl_ms <= tombstone_grace_ms` and
   `tombstone_grace_ms >= 2 * max_clock_skew_ms`.

5. **Safe cleanup:** After a ring change, a record can be dropped only when every current owner has confirmed holding that version or a newer one.

6. **Eviction safety:** Tombstones and expired records still within their grace period are never ordinary eviction candidates.

7. **Quorum overlap:** In quorum mode, `read_quorum + write_acks > replicas`.

8. **Membership self-ownership:** A node speaks for itself. Remote claims about its own membership entry never overwrite it; only its own refutation can bump its incarnation.

9. **RPC deadlines:** Every RPC has a timeout. A peer that does not respond must not block a coordinator indefinitely.

10. **Rebalancing fairness:** Rebalancing is rate-limited and yields to the event loop so that background synchronization does not monopolize a node's execution.

These invariants are the core safety properties to check when modifying replication, membership, TTL, eviction or rebalancing code.

## 13. Known Limits

SelfHeal Cache is an educational, in-memory distributed cache. Its mechanisms improve availability and recovery, but the following limitations remain.

### Durability and Recovery

- **No persistence:** A full-cluster restart loses everything. Hints are in-memory, so a stand-in dying before replay loses that write's hint.
- **Synchronization sessions are in-memory:** A restarted joiner must start a fresh synchronization process.
- **No crash-safe recovery:** Data is not backed by a write-ahead log or durable storage.

### Consistency

- **LWW loses truly concurrent writes:** Causally ordered writes are handled through HLC ordering, but concurrent writes are resolved arbitrarily by version ordering.
- **No anti-entropy:** After a partition, replicas that are never read can remain divergent. Read repair is demand-driven; explicit `CLUSTER SYNC` provides historical synchronization. The Day 33 partition experiment is documented in `PARTITIONS.md`.
- **Non-atomic operations:** `SET NX`, `SET XX`, `EXPIRE`, `PEXPIRE` and `PERSIST` use read-modify-write behavior and are not atomic cluster-wide.
- **Quorum is not linearizable:** Failed quorum writes are not rolled back and can resurface later.
- **Eviction voids quorum overlap:** The read-after-write overlap argument assumes eviction is disabled.
- **Sloppy-quorum visibility gap:** A write acknowledged through a stand-in may not immediately be visible through the original preference list.

### Membership and Rebalancing

- **Join-window assumptions:** A coordinator partitioned from gossip for longer than `rebalance_settle_ms` can write without including the joiner.
- **Incomplete joins:** A node can become `ALIVE` after `join_max_wait_ms` with incomplete historical data.
- **Full-table gossip:** Suitable for roughly 50 nodes; larger clusters need piggybacked membership deltas.
- **Imperfect failure detection:** A slow node can be suspected or declared dead even when it has not crashed.
- **Incarnation uses wall clock:** A manual clock rewind across a restart is unsupported because incarnation persistence is not implemented.
- **Cleanup can be delayed:** An unreachable owner cannot confirm its version, so obsolete records must be retained.

### Performance and Resource Management

- **Single-threaded asyncio per node:** One large scan or slow handler can delay that node's traffic. Scans and background work are chunked and paced, but the effect is not eliminated.
- **Approximate memory cap:** The eviction budget is not a hard process RSS limit.
- **JSON/base64 RPC overhead:** Encoding large values costs approximately 33% in payload overhead. MessagePack could reduce this cost.
- **No WAN measurements:** Network latency, clock differences and failure behavior across geographically distributed machines have not been established.

### Security and Deployment

- **Trusted network:** There is no authentication, authorization or TLS.
- **Single-host testing:** Docker containers share a host clock and have near-zero network latency compared with a real distributed deployment.
- **No production guarantee:** Passing the included tests demonstrates behavior under their specific configurations and failure scenarios, not universal correctness.

## 14. Decision Log

The following decisions summarize the architecture and implementation choices. They record the final design rather than the chronological development process.

| # | Decision | Rationale |
|---:|---|---|
| 1 | Use Python 3.12 and `asyncio` | Keep the implementation focused on distributed-systems behavior without introducing unnecessary infrastructure. |
| 2 | Use a leaderless, Dynamo-style architecture | Any node can coordinate requests; no permanent primary is required. |
| 3 | Keep RESP and internal RPC on separate interfaces | Preserve Redis client compatibility without coupling internal communication to RESP. |
| 4 | Use length-prefixed JSON for internal RPC | Provide a simple, explicit framing protocol for node communication. |
| 5 | Use MD5 for deterministic 64-bit ring positions | Provide stable non-cryptographic hashing. |
| 6 | Use 150 vnodes per physical node | Balance ownership distribution against ring construction and lookup cost. |
| 7 | Use stable node IDs and `{node_id}#{i}` token names | Keep ring positions deterministic across restarts. |
| 8 | Use distinct physical nodes in preference lists | Avoid placing multiple replicas on the same machine. |
| 9 | Separate liveness from ring ownership | Prevent temporary failures from causing repeated reshuffling. |
| 10 | Use incarnation and ranked membership states | Resolve gossip updates deterministically and allow self-refutation. |
| 11 | Use SWIM-style direct and indirect probing | Reduce false suspicions caused by one failed direct probe. |
| 12 | Use full-table push-pull gossip | Keep membership dissemination simple for small clusters. |
| 13 | Use HLC integer versions with node-ID tie-breaking | Preserve monotonic logical ordering and deterministic conflict resolution. |
| 14 | Use absolute TTL deadlines | Preserve expiration semantics through replication, repair and transfer. |
| 15 | Represent deletion with tombstones | Prevent stale replicas from resurrecting deleted values. |
| 16 | Coordinate hint lifetime with tombstone grace | Prevent delayed hints from reviving garbage-collected records. |
| 17 | Use hedged reads and configurable read fanout | Reduce tail latency while consulting multiple replicas. |
| 18 | Use sloppy quorum and hinted handoff in eventual mode | Keep writes available when preference-list owners are unavailable. |
| 19 | Bound hint storage and reject new hints when full | Avoid silently discarding existing recovery work. |
| 20 | Keep read repair demand-driven | Repair stale or missing copies encountered during reads without a full background scan. |
| 21 | Use pull-based joins | Let joining nodes retrieve historical data from multiple available sources. |
| 22 | Let joining nodes receive writes but demote them in reads | Avoid losing updates during historical transfer without serving incomplete data as a normal read target. |
| 23 | Use a settle window before ownership transitions | Allow gossip to disseminate membership changes before rebalancing. |
| 24 | Double-write to future owners during graceful leave | Deliver current updates while the leaving node drains historical records. |
| 25 | Require owner confirmation before cleanup deletion | Avoid deleting the last available or newest copy of a record. |
| 26 | Provide explicit `CLUSTER FORGET` and `CLUSTER SYNC` | Separate administrative membership removal from historical data restoration. |
| 27 | Use HLC for versions and physical time for TTL | Keep logical ordering separate from absolute expiration. |
| 28 | Reject excessively future remote HLC timestamps | Prevent badly skewed clocks from pushing other nodes arbitrarily into the future. |
| 29 | Use pinned tombstones and a soft memory cap | Prioritize deletion safety over strict memory-budget enforcement. |
| 30 | Support both LRU and LFU | Compare recency-based and frequency-based eviction behavior. |
| 31 | Add strict preference-list quorum mode | Provide an optional read/write overlap guarantee without sloppy acknowledgements. |
| 32 | Qualify quorum guarantees when eviction is enabled | A removed replica can invalidate the overlap argument. |
| 33 | Use worker-owned keyspaces and a conservative oracle | Avoid false correctness failures from concurrent worker writes and ambiguous request outcomes. |
| 34 | Separate strict and informational chaos scenarios | Distinguish violated guarantees from expected consistency trade-offs. |
| 35 | Keep stale reads, retries, failures and lost writes as separate metrics | Prevent misleading interpretations of distributed-system test results. |
| 36 | Rate-limit and yield during background rebalancing | Keep synchronization work from monopolizing the asyncio event loop. |

---

*This document describes the implemented design, its assumptions and its measured behavior. All guarantees are conditional on the stated configuration and failure assumptions. Benchmarks and tests are evidence for specific executions, not universal correctness or performance guarantees.*