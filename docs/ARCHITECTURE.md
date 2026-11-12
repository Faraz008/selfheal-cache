# Architecture

```mermaid
flowchart LR
  C[Smart client<br/>seed list, retry-once,<br/>penalty box] -->|RESP| N1
  subgraph Cluster
    direction TB
    N1[n1] <-->|gossip + SWIM probes| N2[n2]
    N2 <-->|gossip + SWIM probes| N3[n3]
    N3 <-->|gossip + SWIM probes| N1
    N1 -. RPC: put / get / hint / stream .- N2
    N2 -. RPC .- N3
    N3 -. RPC .- N1
  end
```

## Inside one node

```mermaid
flowchart TB
  R[RESP server<br/>client port] --> CO[Coordinator]
  CO --> RING[Ring<br/>consistent hash + vnodes]
  CO --> ST[(Store<br/>versioned records,<br/>TTL, tombstones, LRU/LFU)]
  CO --> HS[(Hint store)]
  CO <--> RPC[RPC layer<br/>multiplexed, timeouts]
  MEM[Membership table<br/>ALIVE/SUSPECT/DEAD/LEFT] --> CO
  GOS[Gossip] --> MEM
  DET[Failure detector] --> MEM
  REB[Rebalancer<br/>join / leave / cleanup] --> ST
  REB --> RPC
  HR[Hint replayer] --> HS
  HR --> RPC
  CLK[Hybrid logical clock] --> CO
```

## Life of a write (eventual mode)

1. Any node accepts `SET k v`. It becomes the **coordinator**.
2. It stamps one HLC **version** and computes one absolute `expire_at`.
3. `ring.get_preference_list(k, N)` gives N distinct replicas.
4. It sends `put` to all N in parallel and returns after **W** acks.
5. A replica that is DEAD or fails gets a **stand-in** from further round the ring, which stores a **hint**.
6. When the owner is ALIVE again the hint is replayed. Read repair mops up the rest.

## Life of a read

1. The coordinator orders replicas (ALIVE first, DEAD last, JOINING demoted).
2. It asks `read_fanout` (or R in quorum mode) replicas, hedging after `hedge_ms`.
3. The newest version wins. Stale owners are repaired in the background (or before replying, in quorum mode).