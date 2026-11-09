import asyncio
import logging
import random

from .clock import HybridClock
from .config import NodeConfig
from .gossip import Gossiper
from .membership import Entry, Membership, State
from .rpc import Peers, RpcError

log = logging.getLogger("detector")


class FailureDetector:
    def __init__(
        self,
        cfg: NodeConfig,
        membership: Membership,
        peers: Peers,
        gossiper: Gossiper,
        clock: HybridClock | None = None,
    ):
        self.cfg, self.m, self.peers, self.gossiper, self.clock = (
            cfg,
            membership,
            peers,
            gossiper,
            clock,
        )
        self.interval = cfg.probe_interval_ms / 1000
        self.probe_timeout = cfg.probe_timeout_ms / 1000
        self.suspect_timeout = cfg.suspect_timeout_ms / 1000
        self.rng = random.Random()
        self._order: list[str] = []
        self._task: asyncio.Task | None = None
        self._bg: set[asyncio.Task] = set()

    def handlers(self):
        return {"ping": self._rpc_ping, "ping_req": self._rpc_ping_req}

    async def _rpc_ping(self, a):
        return {"entry": self.m.self_entry.to_wire()}

    async def _rpc_ping_req(self, a):
        """Ping `target` on behalf of a caller who can't reach it."""
        try:
            await self.peers.call(a["target"], "ping", timeout=self.probe_timeout)
            return True
        except RpcError:
            return False

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        tasks = [t for t in (self._task, *self._bg) if t]
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._task = None

    # ---- probing ----
    def _next_target(self) -> str | None:
        while True:
            if not self._order:
                self._order = [e.node_id for e in self.m.others() if e.state is not State.DEAD]
                if not self._order:
                    return None
                self.rng.shuffle(self._order)
            nid = self._order.pop()
            e = self.m.get(nid)
            if e is not None and e.state is not State.DEAD:
                return nid

    async def _ping(self, nid: str) -> bool:
        try:
            res = await self.peers.call(nid, "ping", timeout=self.probe_timeout)
        except RpcError:
            return False
        if self.clock is not None:
            self.clock.observe(res.get("hlc", 0))
        self.m.merge([Entry.from_wire(res["entry"])])  # picks up a refuted incarnation
        return True

    async def _ping_via(self, helper: str, nid: str) -> bool:
        try:
            # wait a bit longer than the helper's own ping timeout
            return bool(
                await self.peers.call(
                    helper, "ping_req", {"target": nid}, timeout=self.probe_timeout * 1.5
                )
            )
        except RpcError:
            return False

    async def probe(self, nid: str) -> bool:
        if await self._ping(nid):
            return True
        helpers = [
            e.node_id for e in self.m.others() if e.node_id != nid and e.state is State.ALIVE
        ]
        helpers = self.rng.sample(helpers, min(self.cfg.indirect_probes, len(helpers)))
        if helpers:
            results = await asyncio.gather(*(self._ping_via(h, nid) for h in helpers))
            if any(results):
                log.info(
                    "direct ping to %s failed but indirect succeeded (bad path, not bad node)", nid
                )
                return True
        return False

    def _expire(self) -> None:
        for nid in self.m.expire_suspects(self.suspect_timeout):
            log.warning("declared %s DEAD (suspected for %.1fs)", nid, self.suspect_timeout)

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.interval * self.rng.uniform(0.9, 1.1))
            try:
                self._expire()
                nid = self._next_target()
                if nid is not None and not await self.probe(nid):
                    if self.m.mark_suspect(nid):
                        log.warning("suspecting %s", nid)
                        t = asyncio.create_task(self.gossiper.exchange(nid))  # tell the accused
                        self._bg.add(t)
                        t.add_done_callback(self._bg.discard)
            except Exception:
                log.exception("probe round failed")

    async def _rpc_ping(self, a):
        return {"entry": self.m.self_entry.to_wire(), "hlc": self.clock.last if self.clock else 0}
