selfheal-cache: Design
This document records what the system does, why, and what it does not do. Every guarantee here is backed by a test or a recorded run listed in CLAIMS.md. Measured numbers live in the benchmark files, not here.

Contents
Goals and non-goals
Architecture overview
Data model
Placement
Membership and failure detection
Replication
Rebalancing
Time, TTL and clocks
Eviction
Consistency modes
Anti-entropy
Persistence
Security
Deployment
Verification
Invariants
Known limits
Decision log
1. Goals and non-goals
Goals

Several independent nodes form one logical cache.
Consistent hashing, so membership changes move a minimal number of keys.
Every key lives on at least one additional node; reads survive loss of the primary.
Failures are detected automatically and traffic is rerouted without manual action.
Joining or returning nodes rebalance with zero client-visible downtime.
TTLs are honoured consistently across all replicas.
A live kill mid-traffic causes zero failed client requests beyond one configurable retry.
Stretch: LRU/LFU eviction; optional strict quorum mode.
Added beyond the brief: persistence, anti-entropy, per-request consistency levels, session guarantees, mutual TLS and authentication, Kubernetes deployment, signed CI/CD.

Non-goals

Linearizability, transactions, compare-and-swap.
Being a database: nothing promises that an acknowledged write is unconditionally durable.
Multi-datacenter operation, rack/zone-aware placement, encryption at rest, key-level ACLs.
Assumptions: clocks are NTP-synced within max_clock_skew_ms (default 5 s); members are trusted once they hold a CA-signed certificate; block storage (not NFS) backs data volumes.

2. Architecture overview
Leaderless, Dynamo-style. Any node accepts any request and acts as its coordinator. There is no master and no single point of failure.

RESP serverclient port
Coordinator
Ringconsistent hash + vnodes
Storerecords, TTL, tombstones,LRU/LFU
Hint store
RPC layermultiplexed, timeouts, mTLS
Membership table
Gossip
Failure detector
Rebalancer
Anti-entropy
Hint replayer
Hybrid logical clock
WAL + snapshots
Admin HTTPhealth, metrics
Two ports per node. The client port speaks RESP (so redis-cli works). The RPC port carries node-to-node traffic as 4-byte length-prefixed JSON, with binary values base64-encoded. One multiplexed connection per peer, request IDs match replies to callers, and every call has a timeout.

Single-threaded asyncio per node, plus one WAL thread and one snapshot thread. Long scans (rebalancing, anti-entropy, snapshots) are chunked and yield to the loop.

Life of a write (eventual mode)
Any node accepts SET k v and becomes coordinator.
It stamps one HLC version and computes one absolute expire_at.
The preference list gives N distinct replicas.
It sends put to all N in parallel and returns after W acks.
A dead or failing owner's slot goes to a stand-in further round the ring, which stores a hint.
When the owner is ALIVE again, the hint replays; read repair mops up the rest.
Life of a read
Replicas are ordered ALIVE, SUSPECT, JOINING, DEAD.
The coordinator asks read_fanout of them, launching one more if nothing answers within hedge_ms.
The newest version wins. Stale owners are repaired (in the background in eventual mode, before replying in strict mode).
3. Data model
A Record is (value | tombstone, version, expire_at), immutable.

Version = (hlc_timestamp, node_id). Tuple comparison: higher timestamp wins, ties broken by node id, so every replica picks the same winner regardless of arrival order.
Tombstone = value=None. A delete is a write; its expire_at is the delete time.
expire_at is an absolute wall-clock millisecond timestamp, computed once by the coordinator. A countdown would restart at every hop (replication, hints, repair, streaming), so replicas would expire at different times.
Store.apply is last-write-wins and idempotent. It is the only way data enters the store, which is why one journaling hook covers every write path.
Expired values and tombstones are kept until expire_at + grace. Otherwise a stale replica could push an old value back after it expired or was deleted. Past the grace period they are garbage-collected.
Collectable records are never shipped. Expired values ship as bare tombstones (same version, no payload) during rebalancing and snapshots.
4. Placement
Ring: 64-bit hash space, 150 virtual nodes per node. Tokens are hash64("{node_id}#{i}"), with hash64 the first 8 bytes of MD5 (non-cryptographic use). Python's hash() is banned because it is salted per process, which would give every node a different ring.
Node ids must be stable and unique. A changed id owns different ranges and reshuffles everything.
Preference list: the first N distinct physical nodes clockwise from the key's hash. Returning N tokens instead would replicate a key to the same node through neighbouring vnodes. Fewer than N nodes returns all of them (a degraded but legal state).
walk(key) yields every distinct node once; the first N are the preference list and the rest are stand-in candidates.
Rebuilds happen only on membership change and cost O(V log V); lookups are O(log V).
Range diff: because the owner list is constant between consecutive tokens, the union of two rings' tokens gives cut points; comparing replica sets per interval gives exactly which ranges move, who gains, and who loses.
Measured properties (tests and charts): adding a node moves about 1/(N+1) of keys, only onto the new node; removing a node moves only its keys; load imbalance falls roughly as 1/√vnodes.
5. Membership and failure detection
Table
An Entry is (node_id, host, port, rpc_port, state, incarnation). States rank JOINING < ALIVE < SUSPECT < LEAVING < DEAD < LEFT.

Merge rule: a remote entry wins iff it has a higher incarnation, or an equal incarnation and a higher-ranked state. This is a total order, so merging is max: commutative, associative, idempotent. Gossip can arrive duplicated and out of order and tables still converge.

Refutation: if someone claims I am SUSPECT, DEAD or LEFT at an incarnation at or above mine, I set my incarnation to theirs + 1 and state ALIVE. That outranks the accusation everywhere. A node that is LEAVING or LEFT does not refute.

Suspicion timers are local, never gossiped. Each node starts its own timer on first learning of a suspicion, and every node derives the same (incarnation, DEAD) entry.

Incarnation starts at boot time (ms). A restarted node outranks the DEAD record of its previous life with no persisted state. A manual wall-clock rewind across a restart is unsupported.

Probing (SWIM-style, every probe_interval_ms)
Pick the next target from a shuffled round-robin of non-DEAD members (every node is probed within N-1 periods).
Direct ping with probe_timeout_ms; a reply carries the target's own entry.
On failure, ask indirect_probes (2) other ALIVE nodes to ping the target. Any success means our path is bad, not the node.
If all fail, mark SUSPECT locally and immediately gossip to the accused so a live-but-slow node refutes within one round trip.
After suspect_timeout_ms (5 s) the node is DEAD.
Expected convergence: about 6-8 s with default timings.

Gossip
Full-table push-pull to gossip_fanout (2) random peers each second, with jitter; a 10% chance per round to also contact a DEAD node so a falsely-DEAD node can hear the rumor and refute. A gossip failure is not a failure verdict; only the detector issues verdicts, so there is one place to reason about false positives.

Ring is not liveness
DEAD nodes stay in the ring. Only routing skips them. A flapping node therefore never reshuffles ownership. The ring changes only when a node is first seen, or becomes LEFT (an explicit, terminal removal).

Bootstrap
A new node needs one seed address. It contacts seeds with exponential backoff; the seed merges its entry and replies with its whole table. Join and rejoin share one code path. A node never seeds from itself.

6. Replication
Write path
Coordinator sends to all N preference-list owners in parallel and returns after W acks (default N=3, W=2).
Sloppy quorum: an owner that is DEAD, or whose RPC just failed, is replaced by the next healthy serving node past the preference list (one shared iterator, so each failed slot gets a different stand-in), which stores a hint. This covers the window before the detector declares death.
Hints: newest version per (owner, key) only; capped (max_hints, new keys refused at the cap, not silently evicting); TTL'd (hint_ttl_ms); persisted in a log. They replay in batches once the owner is ALIVE (also triggered immediately by a membership change), and are deleted only after the owner confirms the RPC. Replay is idempotent because of LWW.
Not rolled back on failure: a write that reached fewer than W replicas returns an error but stays applied where it landed.
Read path
Ordering: ALIVE, SUSPECT, JOINING, DEAD. DEAD nodes are tried last, never skipped, because the detector can be wrong.
Hedging: if nothing has answered within hedge_ms (50), the next replica is launched; losers are cancelled and awaited.
Newest wins (a None answer never beats a value). Tombstones and expired records are returned raw so liveness is decided by the coordinator.
Read repair: only owners that answered and were stale are repaired. Repair pushes tombstones and expired records too, which is what stops resurrection.
During a join, reads widen to read_fanout + joining candidates, including the displaced old replica (a JOINING node may hold the only copy of a fresh W=1 write).
Client library
Seeds a node list, learns the rest from CLUSTER NODES, refreshes every 5 s, round-robins healthy nodes, and puts a failed node in a penalty box. On connection error, timeout or CLUSTERDOWN it retries once on a different node. Real command errors and auth errors are not retried. Retrying is safe because operations are idempotent under LWW, except SET NX/XX.

7. Rebalancing
Flow	Who moves data	How
Join / restart	The new node pulls	Streams its ranges from a live source, stays JOINING until done, then ALIVE
Graceful leave	The leaver pushes	LEAVING, hands every record to its future owners, then LEFT
Cleanup	The ex-owner pushes	Drops records it no longer owns, only after every current owner confirms
Decisions:

Pull for joins. The joiner knows when it is complete and decides when to go ALIVE.
Settle delay (rebalance_settle_ms) before pulling, so every coordinator knows about the joiner and includes it in writes.
Source selection: current owners first, then old owners, so the source really holds the data in both a fresh join and a restart; failing sources fall through to the next candidate.
Double-writes while LEAVING: coordinators also write (best effort, not counted toward W) to the future owners computed from the ring minus leaving nodes.
Deletion needs confirmation: the ex-owner asks each owner "do you hold version ≥ mine?", pushes what is missing, and drops its copy (drop_if_version, so a newer write survives) only if every owner answered. An unreachable owner means keep and retry.
Cleanup gating: ALIVE self, no JOINING nodes, no LEAVING nodes, ring stable for cleanup_delay_ms.
Pacing: a Pacer caps average records per second; scans yield every 2,000 keys; stream pages are small (500 records); stream sessions are in-memory with a TTL.
Availability over completeness: if no source is reachable within join_max_wait_ms, the node goes ALIVE with gaps (logged as an error); read repair and CLUSTER SYNC heal them.
After CLUSTER FORGET of a dead node, there is no automatic re-replication; run CLUSTER SYNC on survivors, or enable anti-entropy.
8. Time, TTL and clocks
Two time axes, never mixed: TTL deadlines are wall-clock milliseconds; versions are HLC integers.

expire_at is created in exactly one place: the coordinator's command handler (relative EX/PX converted once; EXAT/PXAT/EXPIREAT/PEXPIREAT supplied by the client). Every other component carries it unchanged. A replica uses its clock only to decide liveness or GC of a record it already holds. A tripwire test fails if any other module builds expire_at from a clock.

Hybrid logical clock: ts = (physical_ms << 16) | counter. It follows the wall clock, never goes backwards, and observe() pulls it past any timestamp seen from another node, so a write made after seeing version V always outranks V whatever the wall clocks say. It spreads on every received record, on gossip, and on ping replies. Drift guard: a remote timestamp more than max_clock_skew_ms ahead of local wall time is refused, counted and logged.

Why not max(wall_ms, last+1)? My first version did that: above 1,000 writes/s per coordinator it ran ahead of the wall clock without bound. The 16-bit counter pushes that threshold to tens of millions of writes per second.

What skew does:

Effect of skew S	Behaviour
TTL edge	key expires at the same absolute deadline everywhere, observed within ±S (tested and measured)
Causally ordered writes	never inverted (HLC); a negative-control test reproduces the inversion without it
Truly concurrent writes	arbitrary LWW winner; one is lost
Tombstone / expired GC	safe while grace ≥ 2 × S (enforced at startup)
Crazy-clock node	its far-future versions beat honest later writes on keys it wrote; spread is blocked and logged; prevention is NTP
Incarnations	still wall-clock; a manual rewind across a restart is unsupported
There is deliberately no skew tolerance inside is_live: a tolerance moves the edge for everyone without making two clocks agree.

Non-atomic operations: SET NX/XX, EXPIRE, PERSIST are read-then-write. EXPIRE can overwrite a concurrent SET of the same key with the old value. A TTL-only "touch" record that merges field-wise would fix this (future work).

9. Eviction
Exact LRU or LFU (deterministic, unit-testable). LFU uses frequency buckets, a saturating counter (255), new keys start at 3, and periodic halving so old hot keys age out.
Only live values are evictable. Tombstones and in-grace expired records are pinned: evicting them lets a stale replica resurrect the key. Under pressure an expired value is shrunk to a bare tombstone (payload dropped, version kept).
The cap is soft. If pinned records alone exceed it, the node overshoots and counts evict_blocked; refusing replication writes would make replicas diverge.
Accounting is approximate: len(key) + len(value) + OVERHEAD, with OVERHEAD calibrated against real allocations. RSS is larger than accounted bytes, so set max_memory to roughly 50% of the container limit.
Eviction is purely local: no tombstone, no re-replication, no hints. An evicted key is a cache miss on that node; read repair may refill it.
Only client-serving reads refresh recency. Streams, handoff and repair do not.
Not logged to the WAL; replay re-derives it by passing through apply.
Incompatible with anti-entropy (an evicted key looks missing and would be refilled forever; refused at startup) and voids the quorum overlap guarantee.
10. Consistency modes
Cluster default: eventual or quorum. Per request: CL suffix on SET, GET, DEL, overriding the default.

eventual (default)	quorum / explicit level
Write succeeds when	W acks; stand-ins and hints count	k acks from the preference list only (no stand-ins, no hints)
Read succeeds when	any read_fanout replicas answer	k replicas answer, otherwise error
Stale replica found	repaired in the background	repaired before replying
Minority side of a partition	keeps accepting writes, diverges	refuses, per key
Guarantee	convergence	after an acked write, a read sees it or something newer (stable ring, no eviction)
Does not give	freshness	linearizability, CAS, atomic EXPIRE/NX, rollback of failed writes
Levels: ANY (writes only, 1 ack, sloppy), ONE, QUORUM (= N//2+1), ALL, or an integer 1..N, clamped to the replicas that exist. Any explicit level except ANY is strict. level_floor stops callers going below the cluster's configured W/R and forbids ANY.

Overlap rule: R + W > N, with ANY never overlapping. Validated at startup in quorum mode. A 9-case test builds an adversarial lag (the write is acked by the smallest possible set; the read asks the stalest replicas first) and asserts a stale read appears exactly when the rule allows it.

Overlap argument: a write acked by W nodes of the preference list and a read asking R + joining of the N + joining widened candidates share at least R + W - N ≥ 1 node.

Failure reality: ALL has no failover (every coordinator needs the same dead replica). In a 3|2 partition availability is per key, because a key's replicas straddle the split as 3+0 or 2+1. Failed quorum writes are not rolled back, so "ghost" writes can still surface later.

Session guarantees
Per-key, via client-held version tokens:

GET ... WITHVER returns [value, version] (the version may belong to a tombstone). SET ... WITHVER returns the version. DELV returns [count, version].
GET ... MINVER v: if the newest answer is older than v, widen to every preference-list replica; if none reachable has v or newer, return -STALE, never an older value.
SET/DELV ... AFTER v: the coordinator observes v before stamping, so the write outranks the session's history even through a slow-clock node.
Two relaxations for "absent" records: the token is older than grace/2 (assume collected as dead), or eviction is enabled (a miss is legitimate).
Hint-acked writes are not session-visible until replay.
The client keeps an LRU of tokens (an evicted entry loses its guarantee, counted), and tokens export to JSON.
Guarantee	How	Fails when
Read-your-writes (per key)	write version in the token, MINVER on reads	holder unreachable (-STALE); write was hint-acked
Monotonic reads (per key)	every read version joins the token	token entry dropped by client LRU
Monotonic writes / writes-follow-reads	AFTER	AFTER beyond drift limit
Cross-key causality, linearizability	not provided	always
11. Anti-entropy
Optional (anti_entropy: true). Repairs divergence that no read ever touches (measured after partitions).

One digest tree per peer, built by a single scan of the store: for each key whose preference list contains both me and the peer, hash (key, version) into one of 1,024 buckets chosen by the key's hash. Levels: 1 root, 32 mids, 1,024 leaves, each the XOR of its children. XOR of per-entry 64-bit hashes is an order-independent set-hash.
The hash covers (key, version), never the value, so tombstones, expired and live values compare by version and deletes reconcile.
Protocol: compare roots (one small RPC per peer when in sync), then mids, then leaves, then exchange (key, version) lists for differing buckets and push/pull the newer record each way via put_many / ae_fetch. Everything goes through apply, so it converges to the newest version and never rolls anything back.
Gating: self ALIVE; no JOINING/LEAVING members; ring stable; only ALIVE peers; ring-identity hash must match or the peer is skipped; paced; interval jittered.
Spurious mismatches under write load are expected (digests are cached up to ae_cache_ms) and counted as false positives.
Resurrection guard: a peer that was DEAD longer than grace/2 and returns as the same process (a paused or partitioned node that kept its memory) is quarantined until its boot id changes, because it may hold values whose tombstones were already collected. Residual hole: needs one prior contact.
Cost: O(keys) per round per node (measured in AE_COST.txt). Incremental per-peer trees (XOR toggle on every apply) are the documented next step.
12. Persistence
Optional (data_dir). Before this, a full-cluster restart lost everything.

What is logged: PUT(key, record) and DROP(key, version), inside Store.apply and drop_if_version, so every path is covered and replay is just apply (idempotent, order-tolerant). Hints have their own log (ADD / DONE). Eviction, TTL GC and expired-shrinking are not logged; replay re-derives them. The "newer record already collectable" removal in apply is logged, otherwise a zombie value revives.
Format: binary frames [len u32][crc32 u32][payload]; segment magic SHWAL001 (the trailing digits are the format version; future changes must read 001 or refuse loudly). Zero-length frames are rejected (crc32(b"") == 0 would make zero-filled tails look valid).
Recovery: newest valid snapshot, then later segments in order. A torn tail on the last segment is truncated; corruption in an older segment, or a missing segment, is fatal (CorruptionError), optionally followed by wipe_on_corruption and rejoin. A new segment is always started after recovery. Tested by truncating at every byte offset.
Fsync policies: always (group commit; the ack waits for fsync), everysec (flush every 100 ms, fsync every 1 s), no. An fsync or write failure is fatal (exit code 70): retrying after a failed fsync can silently lose data.
Snapshots: rotate the log to segment N, stream the store into snap-N (fuzzy is fine, replay is idempotent), rename atomically, fsync the directory, and only then delete older segments.
Stale-disk rule: meta.json records last_seen_ms (refreshed every few seconds and on clean stop). If boot finds downtime greater than tombstone_grace_ms / 2, the data dir is wiped and the node rejoins empty. Argument: a tombstone created while the node was down has age ≤ downtime + catch-up time; with both under grace/2 every such tombstone is still alive on peers when the node pulls. Beyond that the node cannot know what it missed.
HLC restore uses only versions this node issued; restoring the maximum of all recovered versions would let one bad peer clock drag ours into the future.
Single writer per directory (flock, POSIX only), and the directory is bound to one node id.
Rebalancing still runs after a restart. It is idempotent and catches the writes missed while down.
Failure	no	everysec	always
Process crash	up to wal_tick_ms per node	up to wal_tick_ms per node	none
OS crash / power loss	whatever the OS had not flushed	up to ~1 s per node	none
Disk lost	everything on that node	same	same (rebuild from replicas)
Whole cluster at once	only if all acking replicas lose it	same	none
Durability is per node: an acked write is durable only on the replicas that acked it.

Audit of earlier "memory only" assumptions

Earlier assumption	Status now
A restarted node is empty	False; join/sync_on_start still pulls (idempotent)
A restarted node holds nothing resurrectable	Safe only because of the wipe rule
Hints die with their holder	Closed by the hint log
HLC restarts from wall clock	Restored from own versions
Cleanup's drop is final	Logged, otherwise leavers resurrect
Failed quorum writes vanish on crash	They are now durable ghosts (same LWW outcome)
13. Security
In scope: a network attacker (passive or active), an unauthenticated client, a low-privilege client trying to escalate. Out of scope: a compromised cluster node, a malicious admin, side channels, heavy-but-authenticated abuse, encryption at rest.

Core stays stdlib-only: ssl for TLS, hashlib.scrypt for passwords. Only certificate generation (tools/pki.py) needs cryptography.
Node-to-node: mutual TLS 1.3. Membership means holding a certificate signed by the cluster CA. Outbound RPC pins identity: the peer's certificate CN must equal the node id we meant to reach (or, in Kubernetes tls_pin: hostname mode, the certificate must match the DNS name dialled).
Client port: optional TLS (server-authenticated; client certificates optional). Passwords carry identity.
Auth: AUTH [user] password; three roles read < write < admin; default-deny: an unclassified command needs admin, and a test fails the build if a real command is missing from the table.
Pre-auth surface: only AUTH and HEALTH, bulks ≤ 512 bytes, ≤ 3 arguments, dropped after unauth_timeout_ms, closed after auth_max_failures, with growing delay per failure.
Passwords: stored as scrypt hashes (parameters validated against attacker-chosen work factors), checked off the event loop with at most 2 concurrent hashes; an unknown user burns one scrypt anyway so timing does not reveal usernames.
Revocation: roles are looked up on every command, so removing a user and reloading cuts off open sessions on their next command.
Reload (SIGHUP or file watcher): swaps certificates and users with no restart; any failure keeps the old settings; a reload that would disable auth or empty the users file is refused; reload can add trust but not remove it (removing a CA needs a restart).
Refused at startup: users without client TLS (unless allow_plain_auth); client_cert_required without client TLS.
Turning TLS on needs a cluster-wide restart (plain and TLS RPC cannot be mixed); safe with persistence under grace/2.
Documented hole: any node with a CA-signed certificate can forge membership gossip. The cluster fully trusts its members. The shared wildcard certificate used in Kubernetes widens this: one pod compromise exposes the shared key.
14. Deployment
Kubernetes

StatefulSet, node id = pod name, advertised address = headless-service DNS name (stable ids are required by the ring; each node owns a disk).
podManagementPolicy: Parallel. OrderedReady deadlocks first boot because readiness requires ALIVE, which requires finding a seed.
Headless Service with publishNotReadyAddresses: true, so pods resolve each other before they are Ready.
Probes: liveness = the event loop answers (/livez), never cluster state (a correctly JOINING node must not be killed); readiness = ALIVE and not draining (/readyz); a startup probe allows up to 10 minutes because WAL replay blocks the loop before serving.
Config: SELFHEAL_* environment variables (precedence: defaults < file < env < CLI); enableServiceLinks: false (a Service named cache injects CACHE_PORT=tcp://...); a node's own address is dropped from its seed list.
Shutdown: SIGTERM drains (readiness off, wait drain_ms, flush, exit). A pod restart is not a departure. Only CLUSTER LEAVE leaves the ring, used for scale-down; afterwards the process idles (exit_on_leave: false) so the kubelet cannot restart it back into the ring.
Rolling updates one pod at a time with minReadySeconds and a PDB (maxUnavailable: 1); canary via updatePartition. Safe with N=3, W=R=2.
Certificates: cert-manager, one shared wildcard certificate (per-pod certs would need a Certificate per ordinal), renewals picked up by a file watcher (no subPath mount, which never updates).
Observability: /metrics (Prometheus text: member states, requests by class and outcome, latency histogram, hints, memory, WAL, anti-entropy, auth); alert rules for pods not ready, errors, quorum failures, latency, hint backlog, restart loops, wiped-on-boot, clock drift, disk, certificate expiry. The admin port exposes counters only, never keys or values; restrict it with a NetworkPolicy.
Image: multi-stage, non-root (uid 10001), read-only root filesystem, no capabilities, zero runtime dependencies; the chaos stage is never deployed to production.
Chart safety: rendering fails on unsafe combinations (replicas below N, anti-entropy with eviction, non-overlapping quorums, auth without TLS), and a test loads the rendered node.json into NodeConfig.
CI/CD

Every merge passes the same gates as a release: lint, tests on two Python versions, secret and dependency scans, chart tests, image scan, and a kind end-to-end run with chaos. A single ci-ok check protects main.
Rolling upgrade (N-1 to N) and rollback (N to N-1) are CI gates. Compatibility contract: rolling upgrades are supported one minor version at a time; a major version marks an on-disk or RPC change that is not N-1 compatible.
Releases are built once from a tag, signed keylessly (cosign), published with SBOM and provenance, and verified by an admission policy.
Promotion is a reviewed git change (Argo CD): dev, then a green nightly soak, staging, a prod canary, prod. CI holds no cluster credentials. Rollback is a revert.
15. Verification
Oracle workload: each worker owns a disjoint key range, so the expected state of every key is exact. A failed write marks the key UNCERTAIN (it may or may not have applied). TTL keys are not judged within 2 s of their deadline. Verification runs only while workers are paused.
Strict vs informational scenarios. Strict (zero failed requests, zero lost acked writes): single-node kill and freeze, join, leave, rolling restart, scale up/down, image upgrade and rollback. Informational (measured, not promised): double kill, kill-all under everysec, 3|2 partition.
Stale reads are counted, not failed; only failed requests and lost acked writes fail a strict scenario.
Partition study: with iptables, a 3|2 split in eventual mode lets both sides ack conflicting writes (LWW picks one after the heal); quorum mode stops conflicting acks but not ghost writes; anti-entropy converges replicas without rolling back.
Negative controls: the HLC skew test and the eventual-mode stale-read test each include a version that reproduces the bug.
What Docker and kind cannot show: real WAN latency, power loss, real node loss, shared-clock skew.
16. Invariants
The ring changes only on first sight of a node or on LEFT. Liveness never changes it.
expire_at is created only by the coordinator's command handler.
Versions are HLC integers. Wall-clock milliseconds never mix with them.
hint_ttl_ms ≤ tombstone_grace_ms, and tombstone_grace_ms ≥ 2 × max_clock_skew_ms.
A record is dropped after a ring change only when every current owner confirmed holding that version or newer.
Tombstones and in-grace expired records are never evicted.
In quorum mode, read_quorum + write_acks > replicas.
A node speaks for itself: remote claims about my entry never overwrite it (only refutation bumps it).
Every RPC has a timeout.
Rebalancing is rate-limited and yields to the event loop.
Anti-entropy never applies data from a peer that was down longer than grace/2 unless that peer restarted.
Anti-entropy and eviction are mutually exclusive.
With fsync=always, a write is acked only after its record is fsynced on the acking replica.
Log order equals apply order, and replay goes through apply, so replay is idempotent.
A node whose disk is older than grace/2 is wiped, not trusted.
Mid-log corruption is never skipped silently: the node fails, or wipes and rejoins.
An fsync or write failure is fatal.
Only versions this node issued restore its HLC.
Any explicit consistency level except ANY uses the preference list only.
A read below its requested level is an error, never a partial success.
With level_floor, no request can be weaker than the cluster's W, R or strictness.
A MINVER read never returns a version older than MINVER; it returns the newer one or -STALE.
A write carrying AFTER v is stamped strictly above v.
Token relaxations exist only for absent records older than grace/2 and for eviction misses.
With rpc_tls, no node-to-node byte is processed unless the peer's certificate chains to the cluster CA.
An outbound RPC connection is dropped unless the peer's identity matches the node we meant to reach.
An unauthenticated connection can run only AUTH and HEALTH, reads at most 512-byte bulks and 3 arguments, and is closed after the timeout or too many failures.
A reload never disables auth, never weakens TLS, and keeps the old material on any failure.
Passwords exist only as scrypt hashes, are checked off the event loop, and a login costs one scrypt whether or not the user exists.
Roles are checked against the current users table on every command; unclassified commands need admin.
A node never seeds from itself.
Liveness never depends on cluster state; readiness means ALIVE and not draining.
SIGTERM drains and flushes; only CLUSTER LEAVE departs the ring.
A pod that has left idles and never restarts into the ring.
No release artifact reaches a cluster unless signed by the release workflow's identity.
Production changes arrive only by a reviewed git change; rolling upgrade and rollback across one minor version are tested on every merge.
17. Known limits
Last-writer-wins loses truly concurrent writes. Causally ordered writes are safe (HLC).
Quorum mode is not linearizable. No CAS; failed writes are not rolled back and can surface later; reads can move backwards across coordinators while a lagging write lands.
Levels are not sessions; sessions are per key only, can be refused (-STALE) when the only holder is down or the write was hint-acked, and do not stop other writers overwriting you. ALL has no failover.
Non-atomic operations: SET NX/XX, EXPIRE, PERSIST.
Eviction voids the quorum overlap guarantee and is incompatible with anti-entropy.
Anti-entropy is scan-based (O(keys) per round) and its resurrection guard needs one prior contact.
Join-window assumption: a coordinator partitioned from gossip for longer than rebalance_settle_ms can write without the joiner (healed by cleanup's check and read repair).
Persistence is per node. An unclean stop under everysec/no loses a bounded tail. Recovery time is linear in snapshot plus log size. Power-loss behaviour is reasoned, not measured. flock is POSIX only. Not encrypted at rest.
Full-table gossip scales to roughly 50 nodes, then needs piggybacked deltas.
Single-threaded asyncio per node: one large scan or slow handler delays that node's traffic (chunked and paced, not eliminated). JSON/base64 RPC costs about 33% on large values.
Security: the cluster fully trusts its members (any CA-signed node can forge gossip); shared wildcard certificate in Kubernetes mode; no certificate revocation (rotate short-lived certificates or the CA); no key-level ACLs; no per-IP rate limiting; no audit log beyond failed-AUTH warnings; mixed plain/TLS clusters unsupported; values capped only by the 512 MB RESP limit.
Replica placement is not zone-aware: all N replicas of a key can land in one zone.
No operator: scale-down, forget and disaster recovery are runbook procedures.
Incarnation uses the wall clock: a manual rewind across a restart is unsupported.
Tested on one host: Docker containers share a clock and have near-zero latency; kind proves orchestration, not partitions or real node loss.
18. Decision log (one line each)
Core

Leaderless Dynamo-style over Raft: availability and latency over linearizability, because a lost cache entry is a miss.
RESP on the client port so redis-cli works; separate JSON RPC internally.
Stable node ids; deterministic hashing; never Python hash().
Virtual nodes: 150 per node (diminishing returns beyond ~150-200).
Versions on every record from the start (retrofitting would rewrite everything).
Tombstones and pinned expired records through a grace period.
Membership and survival

SWIM probing plus incarnations instead of heartbeat counters.
Indirect probes before SUSPECT; suspicion window before DEAD.
Ring is not liveness; DEAD nodes stay in the ring.
Hedged reads; newest version wins; read repair only on owners.
Sloppy quorum with stand-ins and persisted, capped, TTL'd hints.
Client retries once on a different node, with a penalty box.
Rebalancing

Pull to join, push to leave, confirmation-gated cleanup.
Settle delay; double-writes during LEAVING; widened reads during JOIN.
LEFT is a terminal state and removes a node from the ring.
Time

Absolute TTL computed once by the coordinator.
HLC replaces the wall-clock-plus-counter version clock; remote drift refused.
No skew tolerance in is_live; grace ≥ 2 × skew enforced.
Consistency

Cluster-wide quorum mode first, then per-request levels (CL suffix, no session state on connections).
Explicit levels are strict except ANY; repair before replying.
Per-key version tokens (not a global timestamp) for sessions; -STALE instead of going backwards; AFTER for monotonic writes.
Storage

Exact LRU/LFU, pinned tombstones, soft cap.
Journal record application (not commands); fsync failure is fatal; stale-disk wipe rule at grace/2.
Anti-entropy by per-peer XOR set-hash over (key, version), quarantine for long-down peers.
Security

stdlib TLS and scrypt; mTLS with identity pinning; default-deny roles; minimal pre-auth surface; reload never fails open.
Operations

StatefulSet with Parallel management; liveness independent of cluster state; SIGTERM drains, CLUSTER LEAVE departs.
No test retries in CI; one required ci-ok check; signed releases verified at admission; GitOps promotion through a soak gate and a prod canary.
Claude finished the response
