# SelfHeal Cache — Design Document

## 1. Goals

SelfHeal Cache is a leaderless, Dynamo-style distributed in-memory key-value store designed to remain available and recover from node failures.

The seven core project goals are:

1. Provide a Redis-compatible RESP interface for client operations.
2. Use a leaderless architecture without a centralized coordinator.
3. Distribute keys across nodes using consistent hashing.
4. Replicate data across multiple nodes for availability.
5. Support eventual consistency with Last-Write-Wins (LWW) conflict resolution by default.
6. Detect node failures and recover through distributed coordination and synchronization.
7. Support TTL-based expiration and anti-entropy repair to remove stale or missing data.

## 2. Architecture

The system follows a leaderless, Dynamo-style architecture. Any node can coordinate a client request; there is no permanent primary node responsible for all writes.

Consistent hashing determines which nodes are responsible for a given key. Data is replicated across a configurable number of nodes.

Each node exposes two separate network interfaces:

- **Client interface:** Uses RESP on the client-facing port to accept Redis-style commands such as SET, GET, and DEL.
- **Internal interface:** Uses a separate internal protocol and port for node-to-node communication, including replication and synchronization.

Keeping the client and internal protocols separate allows client compatibility without coupling distributed-system communication to RESP.

Nodes will eventually use distributed failure detection and anti-entropy mechanisms to identify unavailable peers and repair inconsistent data.

## 3. Guarantees

- **Eventual consistency:** In the default operating mode, replicas may temporarily contain different values but are expected to converge when communication is restored and synchronization completes.
- **Last-Write-Wins (LWW):** Concurrent or conflicting updates are resolved using timestamps. The update with the latest timestamp wins by default.
- **TTL:** Expiration is represented as an absolute Unix timestamp in milliseconds, rather than a relative duration. Replicas therefore share the same expiration deadline instead of restarting the TTL when data is copied.
- **Quorum mode:** Stronger consistency behavior is available when quorum mode is enabled, according to its configured read/write acknowledgement requirements.

## 4. Non-Guarantees

- **No durability or persistence:** Data is held in memory and may be lost when a node or process restarts.
- **No transactions:** Multiple operations are not guaranteed to execute atomically as a single transaction.
- **No default strong consistency:** The default operating mode provides eventual consistency with LWW conflict resolution. Strong consistency is not guaranteed unless quorum mode is enabled.
- **No protection against every failure scenario:** Recovery and availability depend on the number of surviving replicas and the ability of nodes to communicate.

## 5. Assumptions

- **Clock synchronization:** Nodes are assumed to have reasonably synchronized clocks through NTP. LWW conflict resolution and absolute TTL timestamps depend on clock accuracy.
- **Trusted network:** Nodes communicate over a trusted network.
- **No authentication:** Client and internal communication do not include authentication or authorization in the current design.
- **In-memory operation:** The system prioritizes fast access over persistence and crash-safe recovery.

---

*This document describes the intended system design. Features are implemented incrementally; the current single-node implementation does not yet provide all distributed guarantees described above.*

Token names are "{node_id}#{i}". Node IDs must therefore be stable and unique across restarts. If a node restarts with a new ID, it owns different ranges and everything reshuffles. Use IDs like n1, n2, never random UUIDs per boot.

_rebuild is O(V log V) and runs only on membership change (rare), never per request. Lookups are O(log V) via binary search.