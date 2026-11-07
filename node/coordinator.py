import asyncio
import logging
from collections import Counter

from .config import NodeConfig
from .membership import Entry, Membership
from .ring import Ring
from .rpc import Peers, RpcError
from .store import Record, Store, VersionClock
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
    ):
        self.cfg, self.store, self.m, self.peers = (
            cfg,
            store,
            membership,
            peers,
        )
        self.id = cfg.node_id
        self.ring = Ring(cfg.vnodes)
        self.versions = VersionClock(self.id, store.now)
        self._bg: set[asyncio.Task] = set()
        self.stats = Counter()

        self.m.subscribe(self._on_membership)
        self._on_membership(self.m.entries())  # initial sync

    def _on_membership(self, changed: list[Entry]) -> None:
        for e in changed:
            log.info(
                "member %s -> %s (inc %d)",
                e.node_id,
                e.state.value,
                e.incarnation,
            )
            if e.node_id != self.id:
                self.peers.set_addr(e.node_id, e.host, e.rpc_port)

        ids = self.m.node_ids()

        # Ring membership == "ever seen and not removed".
        # Liveness is NOT part of it.
        if ids != self.ring.nodes:
            self.ring.set_nodes(ids)
            log.info("ring rebuilt: %s", ids)

    # ---- helpers ----

    def now(self) -> int:
        return self.store.now()

    def preference_list(self, key: bytes) -> list[str]:
        return self.ring.get_preference_list(key, self.cfg.replicas)

    async def close(self) -> None:
        for t in list(self._bg):
            t.cancel()

    # ---- background task helper ----

    def _spawn(self, coro) -> asyncio.Task:
        t = asyncio.create_task(coro)
        self._bg.add(t)  # strong ref: otherwise the task can be garbage-collected mid-flight
        t.add_done_callback(self._bg.discard)
        return t

    # ---- RPC handlers (this node acting as a REPLICA) ----

    def handlers(self):
        return {
            "put": self._rpc_put,
            "get": self._rpc_get,
        }

    async def _rpc_put(self, a):
        return self.store.apply(
            b64d(a["k"]),
            rec_from_wire(a["r"]),
        )

    async def _rpc_get(self, a):
        rec = self.store.get_record(b64d(a["k"]))
        return None if rec is None else rec_to_wire(rec)

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
        v = self.versions.next()
        await self._replicate(
            key,
            Record(None, v, v[0]),
        )  # tombstone

    def read_order(self, key: bytes) -> list[str]:
        """Preference list, best candidates first: ALIVE, SUSPECT, JOINING, DEAD.

        sorted() is stable, so ring order is preserved within each class.
        """
        return sorted(
            self.preference_list(key),
            key=self.m.read_priority,
        )

    # ---- reads ----

    async def _fetch(self, nid: str, key: bytes) -> Record | None:
        """Raw record (tombstones included) from one replica. Raises RpcError."""
        if nid == self.id:
            return self.store.get_record(key)

        d = await self.peers.call(
            nid,
            "get",
            {"k": b64e(key)},
        )
        return None if d is None else rec_from_wire(d)

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
        """Live record or None. Day 17: first answer wins, hedged."""
        order = self.read_order(key)

        if not order:
            raise ClusterError("empty ring")

        answers, err = await self._gather_answers(
            key,
            order,
            1,
        )

        if not answers:
            raise ClusterError("no replica reachable") from err

        best = self.newest(answers.values())

        return best if best is not None and best.is_live(self.now()) else None

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

    async def _replicate(
        self,
        key: bytes,
        rec: Record,
    ) -> None:
        pref = self.preference_list(key)

        if not pref:
            raise ClusterError("empty ring")

        need = min(self.cfg.write_acks, len(pref))

        targets = [
            n for n in pref if not self.m.is_dead(n)
        ]  # don't pay a timeout for a known-dead node

        if len(targets) < need:
            raise ClusterError(f"only {len(targets)} of {len(pref)} replicas alive, need {need}")

        tasks = []

        for nid in targets:
            t = asyncio.create_task(self._put(nid, key, rec))
            self._bg.add(t)
            t.add_done_callback(self._bg.discard)
            tasks.append(t)

        pending, acks = set(tasks), 0

        while pending and acks < need:
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
            )
            acks += sum(t.result() for t in done)

        if acks < need:
            raise ClusterError(f"write acked by {acks}/{need} replicas")
