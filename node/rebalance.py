import asyncio
import itertools
import logging
import time
from collections import Counter

from .config import NodeConfig
from .coordinator import Coordinator
from .gossip import Gossiper
from .membership import Membership, State
from .ring import Move, RangeSet, Ring, hash64, ring_diff
from .rpc import Peers, RpcError
from .store import Record, Store
from .wire import b64d, b64e, rec_from_wire, rec_to_wire

log = logging.getLogger("rebalance")


class RebalanceError(Exception):
    pass


class SyncError(RebalanceError):
    """Could not pull every range we are responsible for (yet)."""


class Pacer:
    """Caps AVERAGE throughput (records/sec) so a transfer never starves live traffic."""

    def __init__(self, rate: int):
        self.rate = rate
        self._ready = 0.0  # monotonic time by which everything taken so far is 'paid for'

    async def take(self, n: int) -> None:
        if self.rate <= 0 or n <= 0:
            return
        now = time.monotonic()
        self._ready = max(self._ready, now) + n / self.rate  # idle time is not banked
        delay = self._ready - now
        if delay > 0:
            await asyncio.sleep(delay)


class _Session:
    __slots__ = ("keys", "pos", "touched")

    def __init__(self, keys: list[bytes]):
        self.keys, self.pos, self.touched = keys, 0, time.monotonic()


class Rebalancer:
    def __init__(
        self,
        cfg: NodeConfig,
        store: Store,
        membership: Membership,
        coord: Coordinator,
        peers: Peers,
        gossiper: Gossiper,
    ):
        self.cfg, self.store, self.m, self.coord = cfg, store, membership, coord
        self.peers, self.gossip = peers, gossiper
        self.id = cfg.node_id
        self.pacer = Pacer(cfg.transfer_rate)
        self.stats: Counter = Counter()
        self._sessions: dict[int, _Session] = {}
        self._sids = itertools.count(1)
        self._tasks: list[asyncio.Task] = []
        self._leaving = False
        self._clean_version = -1
        self._dirty = False

    def handlers(self):
        return {
            "stream_open": self._rpc_open,
            "stream_next": self._rpc_next,
            "stream_close": self._rpc_close,
            "have_many": self._rpc_have,
        }

    # ---- source side of a stream ----
    async def _scan(self, rs: RangeSet) -> list[bytes]:
        """Keys whose hash falls in rs. Yields to the loop regularly: a big scan must
        never freeze client traffic."""
        out: list[bytes] = []
        for i, k in enumerate(self.store.keys_snapshot()):
            if hash64(k) in rs:
                out.append(k)
            if i % 2000 == 1999:
                await asyncio.sleep(0)
        return out

    def _reap_sessions(self) -> None:
        cutoff = time.monotonic() - self.cfg.session_ttl_ms / 1000
        for sid, s in list(self._sessions.items()):
            if s.touched < cutoff:
                del self._sessions[sid]

    async def _rpc_open(self, a):
        self._reap_sessions()
        rs = RangeSet([(lo, hi) for lo, hi in a["ranges"]])
        keys = await self._scan(rs)
        sid = next(self._sids)
        self._sessions[sid] = _Session(keys)
        return {"sid": sid, "total": len(keys)}

    async def _rpc_next(self, a):
        s = self._sessions.get(a["sid"])
        if s is None:
            raise RebalanceError("unknown or expired stream session")
        s.touched = time.monotonic()
        end = min(s.pos + a["n"], len(s.keys))
        items = []
        for k in s.keys[s.pos : end]:  # read the CURRENT record, not a stale snapshot
            rec = self.store.transferable(k)
            if rec is not None:
                items.append({"k": b64e(k), "r": rec_to_wire(rec)})
        s.pos = end
        done = s.pos >= len(s.keys)
        if done:
            del self._sessions[a["sid"]]
        self.stats["keys_sent"] += len(items)
        return {"items": items, "done": done}

    async def _rpc_close(self, a):
        self._sessions.pop(a["sid"], None)
        return True

    async def _rpc_have(self, a):
        """For each (key, version): do we hold that version or newer?"""
        out = []
        for it in a["items"]:
            rec = self.store.get_record(b64d(it["k"]))
            out.append(rec is not None and rec.version >= (it["ts"], it["n"]))
        return out

    # ---- puller side of a stream ----
    async def _pull_from(self, src: str, ranges: list[tuple[int, int]]) -> int:
        """Stream every record in `ranges` from src into our store (LWW-merged).
        Idempotent: running it twice, or racing live writes, is harmless."""
        opened = await self.peers.call(src, "stream_open", {"ranges": ranges}, timeout=30)
        sid, got = opened["sid"], 0
        try:
            while True:
                res = await self.peers.call(
                    src, "stream_next", {"sid": sid, "n": self.cfg.transfer_batch}, timeout=10
                )
                for it in res["items"]:
                    self.store.apply(b64d(it["k"]), rec_from_wire(it["r"]))
                got += len(res["items"])
                if res["done"]:
                    return got
                await self.pacer.take(len(res["items"]))  # receiver-side rate limit
        finally:
            try:
                await self.peers.call(src, "stream_close", {"sid": sid}, timeout=1)
            except RpcError:
                pass

    async def start(self) -> None:
        self._tasks.append(asyncio.create_task(self._cleanup_loop()))
        if self.cfg.sync_on_start and not self.cfg.seeds and len(self.cfg.member_list()) > 1:
            # Static-member node (re)booting: it is ALIVE immediately, so refill in the background.
            self._tasks.append(asyncio.create_task(self._startup_sync()))

    async def stop(self) -> None:
        for t in self._tasks:
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

    # ---- planning ----
    def _my_moves(self) -> list[Move]:
        """Ranges where I am a replica now but would not be if I did not exist.
        For a brand-new node that is its new ranges; for a restarted node, all of them."""
        others = [n for n in self.coord.ring.nodes if n != self.id]
        without_me = Ring(self.cfg.vnodes)
        without_me.set_nodes(others)
        moves = ring_diff(without_me, self.coord.ring, self.cfg.replicas)
        return [mv for mv in moves if self.id in mv.gained]

    def _sources(self, mv: Move) -> list[str]:
        """Live nodes that hold this range: current owners first, then old owners."""
        out: list[str] = []
        for n in (*mv.new, *mv.old):
            if (
                n != self.id
                and n not in out
                and self.m.state_of(n) in (State.ALIVE, State.SUSPECT, State.LEAVING)
            ):
                out.append(n)
        return sorted(out, key=self.m.read_priority)  # stable: ALIVE first

    async def catch_up(self) -> int:
        """Pull every range this node is responsible for from a live replica.
        Returns records received. Raises SyncError if some range has no reachable source."""
        work: list[tuple[Move, list[str]]] = []
        for mv in self._my_moves():
            cands = self._sources(mv)
            if cands:
                work.append((mv, cands))
            elif len(self.coord.ring) > 1:
                raise SyncError(f"no live source for range {mv.lo:016x}-{mv.hi:016x}")
        total = 0
        queue = [(mv, cands, 0) for mv, cands in work]
        while queue:
            groups: dict[str, list] = {}
            for mv, cands, i in queue:
                groups.setdefault(cands[i], []).append((mv, cands, i))
            queue = []
            for src, items in groups.items():  # one stream session per source
                try:
                    total += await self._pull_from(src, [(mv.lo, mv.hi) for mv, _, _ in items])
                except RpcError as e:
                    log.warning("pull from %s failed: %s", src, e)
                    for mv, cands, i in items:
                        if i + 1 >= len(cands):
                            raise SyncError(f"all sources failed for range {mv.lo:016x}") from e
                        queue.append((mv, cands, i + 1))  # next candidate for those ranges
        self.stats["keys_received"] += total
        return total

    async def _sync_until_done(self) -> None:
        deadline = time.monotonic() + self.cfg.join_max_wait_ms / 1000
        delay = 0.2
        while True:
            try:
                n = await self.catch_up()
                log.info("sync complete: %d records pulled", n)
                return
            except SyncError as e:
                if time.monotonic() >= deadline:
                    # Availability over completeness: a cache may start with gaps.
                    # Read repair and `CLUSTER SYNC` heal them.
                    log.error(
                        "sync incomplete after %dms (%s); continuing", self.cfg.join_max_wait_ms, e
                    )
                    return
                log.warning("sync retry in %.1fs: %s", delay, e)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 5.0)

    # ---- join ----
    async def join(self) -> None:
        """Called by the gossiper once a seed answered. We are JOINING: in the ring and
        receiving writes, but not trusted for reads until our data is here."""
        await asyncio.sleep(self.cfg.rebalance_settle_ms / 1000)  # let our arrival spread
        await self._sync_until_done()
        self.m.activate()
        log.info("join complete: ALIVE holding %d records", self.store.raw_len())

    async def _startup_sync(self) -> None:
        await self._sync_until_done()

    # ---- handoff: the one primitive behind both cleanup and leave ----
    async def _handoff(self, batch: list[tuple[bytes, Record, list[str]]]) -> int:
        """Confirm every owner holds each record (pushing what is missing), then drop
        our copy. A record is dropped ONLY if every one of its owners confirmed."""
        ok = [bool(owners) for _, _, owners in batch]  # no owners at all: never drop
        by_owner: dict[str, list[int]] = {}
        for i, (_, _, owners) in enumerate(batch):
            for o in owners:
                by_owner.setdefault(o, []).append(i)

        async def confirm(owner: str, idxs: list[int]) -> None:
            if self.m.is_dead(owner):
                for i in idxs:
                    ok[i] = False
                return
            try:
                have = await self.peers.call(
                    owner,
                    "have_many",
                    {
                        "items": [
                            {
                                "k": b64e(batch[i][0]),
                                "ts": batch[i][1].version[0],
                                "n": batch[i][1].version[1],
                            }
                            for i in idxs
                        ]
                    },
                    timeout=5,
                )
                missing = [i for i, h in zip(idxs, have) if not h]
                if missing:
                    await self.peers.call(
                        owner,
                        "put_many",
                        {
                            "items": [
                                {"k": b64e(batch[i][0]), "r": rec_to_wire(batch[i][1])}
                                for i in missing
                            ]
                        },
                        timeout=10,
                    )
                    self.stats["keys_pushed"] += len(missing)
            except RpcError as e:
                log.debug("handoff to %s unconfirmed: %s", owner, e)
                for i in idxs:
                    ok[i] = False

        await asyncio.gather(*(confirm(o, ix) for o, ix in by_owner.items()))
        dropped = 0
        for good, (k, rec, _) in zip(ok, batch):
            if good and self.store.drop_if_version(k, rec.version):
                dropped += 1
        self.stats["keys_cleaned"] += dropped
        return dropped

    async def _drain(self, owners_of) -> tuple[int, int]:
        """Walk every record; for those whose owners (per `owners_of`) exclude us, hand
        them off. Returns (examined, dropped)."""
        batch: list[tuple[bytes, Record, list[str]]] = []
        examined = dropped = 0
        for i, k in enumerate(self.store.keys_snapshot()):
            owners = owners_of(k)
            if self.id not in owners:
                rec = self.store.transferable(k)
                if rec is not None:
                    batch.append((k, rec, owners))
            if len(batch) >= self.cfg.cleanup_batch:
                examined += len(batch)
                dropped += await self._handoff(batch)
                await self.pacer.take(len(batch))
                batch = []
            if i % 2000 == 1999:
                await asyncio.sleep(0)
        if batch:
            examined += len(batch)
            dropped += await self._handoff(batch)
        return examined, dropped

    # ---- cleanup after a ring change ----
    def request_cleanup(self) -> None:
        self._dirty = True

    def _cleanup_due(self) -> bool:
        c = self.coord
        if self.m.self_entry.state is not State.ALIVE or c.ring_after is not None:
            return False
        if any(e.state is State.JOINING for e in self.m.entries()):
            return False  # a joiner may still be pulling from us
        if time.monotonic() - c.ring_changed_at < self.cfg.cleanup_delay_ms / 1000:
            return False
        return self._dirty or c.ring_version != self._clean_version

    async def cleanup_once(self) -> tuple[int, int]:
        version = self.coord.ring_version
        self._dirty = False

        def owners_of(k: bytes) -> list[str]:
            pref = self.coord.preference_list(k)
            return pref + self.coord.extras(k, pref)

        examined, dropped = await self._drain(owners_of)
        if dropped < examined:
            self._dirty = True  # some owner was unreachable: retry next round
        else:
            self._clean_version = version
        if examined:
            log.info(
                "cleanup: %d records no longer ours, %d handed off and dropped", examined, dropped
            )
        return examined, dropped

    async def _cleanup_loop(self) -> None:
        interval = self.cfg.cleanup_interval_ms / 1000
        while True:
            await asyncio.sleep(interval)
            self._reap_sessions()
            try:
                if self._cleanup_due():
                    await self.cleanup_once()
            except Exception:
                log.exception("cleanup failed")

    # ---- graceful leave ----
    async def leave(self) -> None:
        """Announce LEAVING, hand every record to its future owners, then become LEFT.
        On failure we revert to ALIVE, so the node is never stuck half-gone."""
        st = self.m.self_entry.state
        if self._leaving or st is State.LEFT:
            raise RebalanceError("already leaving")
        if st is State.JOINING:
            raise RebalanceError("still joining")
        if not [n for n in self.coord.ring.nodes if n != self.id]:
            raise RebalanceError("cannot leave: this is the only node")
        self._leaving = True
        try:
            self.m.begin_leave()  # gossip carries it; peers start double-writing
            await asyncio.sleep(self.cfg.rebalance_settle_ms / 1000)
            deadline = time.monotonic() + self.cfg.leave_timeout_ms / 1000
            n = self.cfg.replicas
            while True:
                after = self.coord.ring_after
                if after is None or not len(after):
                    raise RebalanceError("no node left to take the data")
                examined, dropped = await self._drain(
                    lambda k, ring=after: ring.get_preference_list(k, n)
                )
                log.info("leave pass: %d records examined, %d handed off", examined, dropped)
                if examined == 0:  # a full pass found nothing left to hand off
                    break
                if time.monotonic() > deadline:
                    raise RebalanceError(f"handoff incomplete: {examined - dropped} records remain")
                if dropped < examined:
                    await asyncio.sleep(0.2)
            self.m.finish_leave()
            for _ in range(3):  # push the LEFT verdict out before we go
                await self.gossip.gossip_round()
                await asyncio.sleep(self.cfg.gossip_interval_ms / 2000)
            log.info("left the cluster")
        except BaseException:
            self.m.cancel_leave()  # no-op if we already reached LEFT
            raise
        finally:
            self._leaving = False
