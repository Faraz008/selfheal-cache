import asyncio
import logging
import time
from collections import Counter

from .clock import HybridClock
from .config import NodeConfig
from .hints import HintStore
from .membership import Entry, Membership, State
from .ring import Ring
from .rpc import Peers, RpcError
from .store import Record, Store
from .wire import b64d, b64e, rec_from_wire, rec_to_wire

log = logging.getLogger("coord")


class ClusterError(Exception):
    """Too few replicas reachable to serve this request."""


class Coordinator:
    def __init__(
        self,
        cfg: NodeConfig,
        store: Store,
        membership: Membership,
        peers: Peers,
        hints: HintStore,
    ):
        self.cfg, self.store, self.m, self.peers, self.hints = (
            cfg,
            store,
            membership,
            peers,
            hints,
        )
        self.id = cfg.node_id
        self.ring = Ring(cfg.vnodes)
        self.versions = HybridClock(self.id, store.now, cfg.max_clock_skew_ms)
        self._bg: set[asyncio.Task] = set()
        self.stats = Counter()

        self.ring_after: Ring | None = None  # ring minus LEAVING nodes; None if nobody is leaving
        self.ring_version = 0
        self.ring_changed_at = time.monotonic()
        self.m.subscribe(self._on_membership)
        self._on_membership(self.m.entries())  # initial sync

    def _on_membership(self, changed: list[Entry]) -> None:
        for e in changed:
            log.info("member %s -> %s (inc %d)", e.node_id, e.state.value, e.incarnation)
            if e.node_id != self.id and e.state is not State.LEFT:
                self.peers.set_addr(e.node_id, e.host, e.rpc_port)
        ids = self.m.ring_node_ids()  # everything not LEFT; liveness is NOT part of it
        leaving = set(self.m.leaving_ids())
        bumped = False
        if ids != self.ring.nodes:
            self.ring.set_nodes(ids)
            log.info("ring rebuilt: %s", ids)
            bumped = True
        after_ids = [i for i in ids if i not in leaving] if leaving else None
        if after_ids != (self.ring_after.nodes if self.ring_after is not None else None):
            if after_ids is None:
                self.ring_after = None
            else:
                self.ring_after = Ring(self.cfg.vnodes)
                self.ring_after.set_nodes(after_ids)
            log.info("leaving: %s", sorted(leaving))
            bumped = True
        if bumped:
            self.ring_version += 1
            self.ring_changed_at = time.monotonic()

    # ---- helpers ----

    def now(self) -> int:
        return self.store.now()

    def preference_list(self, key: bytes) -> list[str]:
        return self.ring.get_preference_list(key, self.cfg.replicas)

    async def close(self) -> None:
        for t in list(self._bg):
            t.cancel()

        if self._bg:
            await asyncio.gather(
                *self._bg,
                return_exceptions=True,
            )

    # ---- background task helper ----

    def _spawn(self, coro) -> asyncio.Task:
        t = asyncio.create_task(coro)
        self._bg.add(t)
        t.add_done_callback(self._bg.discard)
        return t

    # ---- RPC handlers (this node acting as a REPLICA) ----

    def handlers(self):
        return {
            "put": self._rpc_put,
            "get": self._rpc_get,
            "hint": self._rpc_hint,
            "put_many": self._rpc_put_many,
        }

    async def _rpc_put(self, a):
        return self.store.apply(b64d(a["k"]), self.observe(rec_from_wire(a["r"])))

    async def _rpc_get(self, a):
        rec = self.store.get_record(b64d(a["k"]), touch=True)
        return None if rec is None else rec_to_wire(rec)

    async def _rpc_hint(self, a):
        return self.hints.add(a["o"], b64d(a["k"]), self.observe(rec_from_wire(a["r"])))

    async def _rpc_put_many(self, a):
        return sum(
            self.store.apply(b64d(i["k"]), self.observe(rec_from_wire(i["r"]))) for i in a["items"]
        )

    # ---- client-facing operations (this node acting as COORDINATOR) ----

    async def set(
        self,
        key: bytes,
        value: bytes,
        expire_at: int | None = None,
    ) -> None:
        await self._replicate(
            key,
            Record(value, self.versions.next(), expire_at),
        )

    async def delete(self, key: bytes) -> None:
        await self._replicate(
            key,
            Record(None, self.versions.next(), self.now()),
        )

    def read_plan(self, key: bytes) -> tuple[list[str], int]:
        """(candidates best-first, number of JOINING replicas). While a replica is joining
        it may hold the only copy of a fresh W=1 write but lacks old data, so we ask it
        AND widen the candidate list with the displaced old replica."""
        pref = self.preference_list(key)
        joining = sum(1 for n in pref if self.m.state_of(n) is State.JOINING)
        cands = self.ring.get_preference_list(key, self.cfg.replicas + joining) if joining else pref
        return sorted(cands, key=self.m.read_priority), joining

    def read_order(self, key: bytes) -> list[str]:
        return self.read_plan(key)[0]

    # ---- reads ----

    async def _fetch(self, nid: str, key: bytes) -> Record | None:
        """Raw record (tombstones included) from one replica. Raises RpcError."""
        if nid == self.id:
            return self.store.get_record(key, touch=True)

        d = await self.peers.call(
            nid,
            "get",
            {"k": b64e(key)},
        )
        return None if d is None else self.observe(rec_from_wire(d))

    async def _gather_answers(
        self,
        key: bytes,
        order: list[str],
        want: int,
    ):
        """Collect `want` replica answers.

        Launches the first `want` replicas at once, and one more whenever:
        (a) a request fails, or
        (b) nothing has answered within hedge_ms (the hedge).

        Returns ({node_id: Record | None}, last_error).
        """
        hedge = self.cfg.hedge_ms / 1000
        queue = list(order)
        running: dict[asyncio.Task, str] = {}
        answers: dict[str, Record | None] = {}
        last_err: Exception | None = None

        def launch() -> None:
            nid = queue.pop(0)
            running[asyncio.create_task(self._fetch(nid, key))] = nid

        try:
            for _ in range(min(want, len(queue))):
                launch()

            while len(answers) < want:
                if not running:
                    if not queue:
                        break
                    launch()

                done, _ = await asyncio.wait(
                    list(running),
                    timeout=hedge if queue else None,
                    return_when=asyncio.FIRST_COMPLETED,
                )

                if not done:  # everyone in flight is slow: hedge
                    launch()
                    continue

                for t in done:
                    nid = running.pop(t)

                    try:
                        answers[nid] = t.result()
                    except RpcError as e:
                        log.warning("read from %s failed: %s", nid, e)
                        last_err = e

                        if queue:
                            launch()

        finally:
            for t in running:
                t.cancel()

            await asyncio.gather(
                *running,
                return_exceptions=True,
            )

        return answers, last_err

    @staticmethod
    def newest(recs) -> Record | None:
        found = [r for r in recs if r is not None]
        return max(found, key=lambda r: r.version) if found else None

    async def read(self, key: bytes) -> Record | None:
        order, joining = self.read_plan(key)
        if not order:
            raise ClusterError("empty ring")
        want = min(self.cfg.read_fanout + joining, len(order))
        answers, err = await self._gather_answers(key, order, want)
        if not answers:
            raise ClusterError("no replica reachable") from err
        best = self.newest(answers.values())
        if best is not None:
            owners = set(self.preference_list(key))
            # repair OWNERS only: the displaced old replica must not be handed a stray copy
            stale = [
                n
                for n, r in answers.items()
                if n in owners and (r is None or r.version < best.version)
            ]
            if stale:
                self._spawn(self._repair(key, best, stale))
        return best if best is not None and best.is_live(self.now()) else None

    async def _repair(self, key: bytes, best: Record, stale: list[str]) -> None:
        # Pushes tombstones and expired records too: that is what stops resurrection.
        results = await asyncio.gather(*(self._put(n, key, best) for n in stale))
        self.stats["read_repairs"] += sum(results)

    # ---- writes and hinted handoff ----

    async def _put(
        self,
        nid: str,
        key: bytes,
        rec: Record,
    ) -> bool:
        try:
            if nid == self.id:
                self.store.apply(key, rec)
            else:
                await self.peers.call(
                    nid,
                    "put",
                    {
                        "k": b64e(key),
                        "r": rec_to_wire(rec),
                    },
                )

            return True

        except RpcError as e:
            log.warning("put to %s failed: %s", nid, e)
            return False

    def extras(self, key: bytes, pref: list[str]) -> list[str]:
        """Nodes that WILL own this key once leaving nodes are gone. They get the write
        now (best effort, not counted toward W) so the handoff has no gap."""
        if self.ring_after is None:
            return []
        return [
            n for n in self.ring_after.get_preference_list(key, self.cfg.replicas) if n not in pref
        ]

    def _standin_iter(self, key: bytes):
        """Healthy SERVING nodes clockwise after the preference list (hints die with
        their holder, so never park one on a joining or leaving node)."""
        for i, nid in enumerate(self.ring.walk(key)):
            if i >= self.cfg.replicas and self.m.state_of(nid) in (State.ALIVE, State.SUSPECT):
                yield nid

    async def _hint_to(
        self,
        sub: str,
        owner: str,
        key: bytes,
        rec: Record,
    ) -> bool:
        try:
            if sub == self.id:
                return self.hints.add(owner, key, rec)

            return bool(
                await self.peers.call(
                    sub,
                    "hint",
                    {
                        "o": owner,
                        "k": b64e(key),
                        "r": rec_to_wire(rec),
                    },
                )
            )

        except RpcError as e:
            log.warning(
                "hint to %s for %s failed: %s",
                sub,
                owner,
                e,
            )
            return False

    async def _write_slot(
        self,
        owner: str,
        key: bytes,
        rec: Record,
        standins,
    ) -> bool:
        """Get `rec` assigned to this owner's slot, directly or via a hint."""
        if not self.m.is_dead(owner) and await self._put(owner, key, rec):
            return True

        for sub in standins:
            if await self._hint_to(sub, owner, key, rec):
                self.stats["hints_written"] += 1
                return True

        return False

    async def _replicate(
        self,
        key: bytes,
        rec: Record,
    ) -> None:
        pref = self.preference_list(key)

        if not pref:
            raise ClusterError("empty ring")

        need = min(self.cfg.write_acks, len(pref))

        for n in self.extras(key, pref):
            if not self.m.is_dead(n):
                self._spawn(self._put(n, key, rec))

        standins = self._standin_iter(key)

        tasks = [
            self._spawn(
                self._write_slot(
                    owner,
                    key,
                    rec,
                    standins,
                )
            )
            for owner in pref
        ]

        pending, acks = set(tasks), 0

        while pending and acks < need:
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
            )
            acks += sum(t.result() for t in done)

        if acks < need:
            raise ClusterError(f"write acked by {acks}/{need} replicas")

    def observe(self, rec: Record) -> Record:
        self.versions.observe(rec.version[0])
        return rec
