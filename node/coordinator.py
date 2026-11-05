import logging

from .config import NodeConfig
from .ring import Ring
from .store import Record, Store, VersionClock

log = logging.getLogger("coord")


class ClusterError(Exception):
    """Too few replicas reachable to serve this request."""


class Coordinator:
    def __init__(self, cfg: NodeConfig, store: Store):
        self.cfg, self.store = cfg, store
        self.id = cfg.node_id
        self.members = {m.node_id: m for m in cfg.member_list()}
        self.ring = Ring(cfg.vnodes)
        self.ring.set_nodes(self.members)
        self.versions = VersionClock(self.id, store.now)

    def now(self) -> int:
        return self.store.now()

    def preference_list(self, key: bytes) -> list[str]:
        return self.ring.get_preference_list(key, self.cfg.replicas)

    async def set(self, key: bytes, value: bytes, expire_at: int | None = None) -> None:
        await self._replicate(key, Record(value, self.versions.next(), expire_at))

    async def delete(self, key: bytes) -> None:
        v = self.versions.next()
        await self._replicate(key, Record(None, v, v[0]))  # tombstone

    async def read(self, key: bytes) -> Record | None:
        return self.store.get_live(key)  # Day 8: local only

    async def _replicate(self, key: bytes, rec: Record) -> None:
        self.store.apply(key, rec)  # Day 8: local only
