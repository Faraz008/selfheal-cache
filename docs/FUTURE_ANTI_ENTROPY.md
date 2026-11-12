# Future work: Merkle-tree anti-entropy

## The gap (measured)
After a 3|2 partition heals, `docs/PARTITIONS.md` shows N divergent keys. A read sweep
(`read_fanout=2`) repairs only replicas that answered; `CLUSTER SYNC` pulls one source per
range. Replicas that nobody reads stay stale indefinitely, because nothing compares them.

## Design
**Unit of comparison:** one tree per *range the node shares with a peer*, not per node. For
the pair (A, B) the shared ranges are the ring ranges where both are in the preference list
(computable with `ring_diff`-style walking over `prefs_at`).

**Tree:** fixed depth (e.g. 2^10 leaf buckets per range) so both sides build identical shapes.
- Leaf bucket = XOR (or sum mod 2^64) of `hash64(key || version)` over records whose key
  hash falls in the bucket. Tombstones are included (same version, no payload) so deletes
  also reconcile. Collectable records are excluded.
- Internal node = hash of its children. XOR-of-hashes leaves make **incremental** update O(1)
  per write: toggle the old entry out, the new one in.

**Protocol (per pair, every `anti_entropy_interval`, e.g. 60 s, jittered):**
1. A → B: `tree_root(range)`. Equal roots: done (the common case, 1 small RPC).
2. Unequal: descend, exchanging children hashes only for mismatching subtrees (~log buckets RPCs).
3. At differing leaves: exchange `(key, version)` lists, then push the newer record each way
   via the existing `put_many` / `have_many` primitives. LWW makes the merge idempotent.
4. Rate-limit with the existing `Pacer`, yield to the loop, and skip pairs where either side is
   JOINING/LEAVING or the ring changed mid-exchange (retry later).

## Interactions to get right
- **GC grace:** a peer that was down longer than `tombstone_grace_ms` may have missed a
  tombstone that was already collected elsewhere; anti-entropy would then resurrect the value.
  Keep the existing rule: such a node must be wiped and fully resynced, not tree-merged.
- **Eviction:** an evicted key looks like a missing key. Anti-entropy would refill it, defeating
  the cap. Either exclude capped nodes' ranges or make refill respect the cap and policy.
- **Quorum mode:** repair of ghost writes (failed writes that landed on one replica) converges
  them to the newest version, which is the same LWW outcome, not a rollback.
- **Cost:** memory for per-range leaf arrays (8 bytes x buckets), CPU for hashing on writes.
  Quantify with a benchmark before enabling by default.

## How we would prove it
Re-run `chaos.partition split_3_2` with anti-entropy on: `after_read_sweep` and
`after_cluster_sync` divergence should reach 0 within a few intervals without any reads, and
`unacked_writes_that_won` is unchanged (anti-entropy converges, it does not roll back).