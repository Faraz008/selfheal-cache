import asyncio
import logging
import random

from .config import NodeConfig
from .membership import Entry, Membership, State
from .rpc import Peers, RpcError

log = logging.getLogger("gossip")


class Gossiper:
    """Push-pull anti-entropy: send my table, receive theirs, merge both ways."""

    def __init__(self, cfg: NodeConfig, membership: Membership, peers: Peers):
        self.cfg, self.m, self.peers = cfg, membership, peers
        self.rng = random.Random()
        self._task: asyncio.Task | None = None

    def handlers(self):
        return {"gossip": self._rpc_gossip}

    async def _rpc_gossip(self, a):
        self.m.merge([Entry.from_wire(d) for d in a["members"]])
        return {"members": self.m.snapshot()}  # reply AFTER merging: caller learns about itself too

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _loop(self) -> None:
        interval = self.cfg.gossip_interval_ms / 1000
        while True:
            await asyncio.sleep(interval * self.rng.uniform(0.8, 1.2))  # jitter avoids lockstep
            try:
                await self.gossip_round()
            except Exception:
                log.exception("gossip round failed")

    def pick_targets(self) -> list[str]:
        live = [e.node_id for e in self.m.others() if e.state is not State.DEAD]
        dead = [e.node_id for e in self.m.others() if e.state is State.DEAD]
        targets = self.rng.sample(live, min(self.cfg.gossip_fanout, len(live)))
        if dead and self.rng.random() < 0.1:
            targets.append(
                self.rng.choice(dead)
            )  # lets a falsely-DEAD node hear the rumor and refute
        return targets

    async def gossip_round(self) -> None:
        await asyncio.gather(
            *(self.exchange(t) for t in self.pick_targets()), return_exceptions=True
        )

    async def exchange(self, node_id: str) -> bool:
        try:
            res = await self.peers.call(node_id, "gossip", {"members": self.m.snapshot()})
        except RpcError as e:
            log.debug("gossip to %s failed: %s", node_id, e)
            return False
        self.m.merge([Entry.from_wire(d) for d in res["members"]])
        return True
