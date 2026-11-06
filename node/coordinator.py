import asyncio
import base64
import logging

from .config import NodeConfig
from .membership import Entry, Membership
from .ring import Ring
from .rpc import Peers, RpcError
from .store import Record, Store, VersionClock

log = logging.getLogger("coord")


class ClusterError(Exception):
    """Too few replicas reachable to serve this request."""


def b64e(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def b64d(s: str) -> bytes:
    return base64.b64decode(s)


def rec_to_wire(r: Record) -> dict:
    return {
        "v": None if r.value is None else b64e(r.value),
        "ts": r.version[0],
        "n": r.version[1],
        "x": r.expire_at,
    }


def rec_from_wire(d: dict) -> Record:
    return Record(None if d["v"] is None else b64d(d["v"]), (d["ts"], d["n"]), d["x"])


class Coordinator:
    def __init__(self, cfg: NodeConfig, store: Store, membership: Membership, peers: Peers):
        self.cfg, self.store, self.m, self.peers = cfg, store, membership, peers
        self.id = cfg.node_id
        self.ring = Ring(cfg.vnodes)
        self.versions = VersionClock(self.id, store.now)
        self._bg: set[asyncio.Task] = set()
        self.m.subscribe(self._on_membership)
        self._on_membership(self.m.entries())  # initial sync

    def _on_membership(self, changed: list[Entry]) -> None:
        for e in changed:
            log.info("member %s -> %s (inc %d)", e.node_id, e.state.value, e.incarnation)
            if e.node_id != self.id:
                self.peers.set_addr(e.node_id, e.host, e.rpc_port)
        ids = self.m.node_ids()
        # Ring membership == "ever seen and not removed". Liveness is NOT part of it.
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

    # ---- RPC handlers (this node acting as a REPLICA) ----
    def handlers(self):
        return {"put": self._rpc_put, "get": self._rpc_get}

    async def _rpc_put(self, a):
        return self.store.apply(b64d(a["k"]), rec_from_wire(a["r"]))

    async def _rpc_get(self, a):
        rec = self.store.get_record(b64d(a["k"]))
        return None if rec is None else rec_to_wire(rec)

    # ---- client-facing operations (this node acting as COORDINATOR) ----
    async def set(self, key: bytes, value: bytes, expire_at: int | None = None) -> None:
        await self._replicate(key, Record(value, self.versions.next(), expire_at))

    async def delete(self, key: bytes) -> None:
        v = self.versions.next()
        await self._replicate(key, Record(None, v, v[0]))  # tombstone

    def read_order(self, key: bytes) -> list[str]:
        """Preference list, best candidates first: ALIVE, SUSPECT, JOINING, DEAD.
        sorted() is stable, so ring order is preserved within each class."""
        return sorted(self.preference_list(key), key=self.m.read_priority)

    async def read(self, key: bytes) -> Record | None:
        """Live record or None. First replica that answers wins."""
        order = self.read_order(key)
        if not order:
            raise ClusterError("empty ring")
        last_err: Exception | None = None
        for nid in order:  # DEAD nodes are tried last, never skipped: we might be wrong
            try:
                if nid == self.id:
                    rec = self.store.get_record(key)
                else:
                    d = await self.peers.call(nid, "get", {"k": b64e(key)})
                    rec = None if d is None else rec_from_wire(d)
            except RpcError as e:
                log.warning("read from %s failed: %s", nid, e)
                last_err = e
                continue
            return rec if rec is not None and rec.is_live(self.now()) else None
        raise ClusterError("no replica reachable") from last_err

    async def _put(self, nid: str, key: bytes, rec: Record) -> bool:
        try:
            if nid == self.id:
                self.store.apply(key, rec)
            else:
                await self.peers.call(nid, "put", {"k": b64e(key), "r": rec_to_wire(rec)})
            return True
        except RpcError as e:
            log.warning("put to %s failed: %s", nid, e)
            return False

    async def _replicate(self, key: bytes, rec: Record) -> None:
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
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
            acks += sum(t.result() for t in done)
        if acks < need:
            raise ClusterError(f"write acked by {acks}/{need} replicas")
