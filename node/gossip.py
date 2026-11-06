import asyncio
import logging
import random

from .config import NodeConfig
from .membership import Entry, Membership, State
from .rpc import Peers, RpcClient, RpcError

log = logging.getLogger("gossip")


class Gossiper:
    """Push-pull anti-entropy: exchange and merge membership tables."""

    def __init__(self, cfg: NodeConfig, membership: Membership, peers: Peers):
        self.cfg = cfg
        self.m = membership
        self.peers = peers

        self.rng = random.Random()

        self._task: asyncio.Task | None = None
        self._boot: asyncio.Task | None = None

    def handlers(self):
        return {"gossip": self._rpc_gossip}

    async def _rpc_gossip(self, a):
        """Receive another node's membership table and return our updated table."""
        self.m.merge([Entry.from_wire(d) for d in a["members"]])

        # Reply after merging so the caller learns about our membership too.
        return {"members": self.m.snapshot()}

    async def start(self) -> None:
        """Start periodic gossip and, if configured, seed bootstrap."""
        self._task = asyncio.create_task(self._loop())

        if self.cfg.seeds:
            self._boot = asyncio.create_task(self._bootstrap())

    async def stop(self) -> None:
        """Cancel both background tasks cleanly."""
        tasks = [t for t in (self._task, self._boot) if t is not None]

        for task in tasks:
            task.cancel()

        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

        self._task = None
        self._boot = None

    async def _bootstrap(self) -> None:
        """
        Contact configured seeds until one responds.

        A joining node only needs one reachable seed address to discover
        the existing cluster.
        """
        delay = 0.2

        while True:
            for host, port in self.cfg.seed_addrs():
                client = RpcClient(
                    host,
                    port,
                    self.cfg.rpc_timeout_ms / 1000,
                )

                try:
                    res = await client.call(
                        "gossip",
                        {"members": self.m.snapshot()},
                    )
                except RpcError as e:
                    log.warning(
                        "seed %s:%d unreachable: %s",
                        host,
                        port,
                        e,
                    )
                    continue
                finally:
                    await client.close()

                # Merge the membership information returned by the seed.
                self.m.merge([Entry.from_wire(d) for d in res["members"]])

                # Activate only after successfully discovering the cluster.
                # Phase 6 will move activation until after data streaming.
                if self.m.self_entry.state is State.JOINING:
                    self.m.activate()

                log.info("joined via seed %s:%d", host, port)
                return

            # No seed answered. Retry with exponential backoff.
            await asyncio.sleep(delay)
            delay = min(delay * 2, 5.0)

    async def _loop(self) -> None:
        """Periodically exchange membership information with other nodes."""
        interval = self.cfg.gossip_interval_ms / 1000

        while True:
            await asyncio.sleep(interval * self.rng.uniform(0.8, 1.2))

            try:
                await self.gossip_round()
            except Exception:
                log.exception("gossip round failed")

    def pick_targets(self) -> list[str]:
        """Choose gossip targets, occasionally including a DEAD node."""
        live = [e.node_id for e in self.m.others() if e.state is not State.DEAD]

        dead = [e.node_id for e in self.m.others() if e.state is State.DEAD]

        targets = self.rng.sample(
            live,
            min(self.cfg.gossip_fanout, len(live)),
        )

        # Give a falsely DEAD node a chance to hear the cluster rumor
        # and refute its suspected failure.
        if dead and self.rng.random() < 0.1:
            targets.append(self.rng.choice(dead))

        return targets

    async def gossip_round(self) -> None:
        """Run one gossip exchange with the selected targets."""
        await asyncio.gather(
            *(self.exchange(node_id) for node_id in self.pick_targets()),
            return_exceptions=True,
        )

    async def exchange(self, node_id: str) -> bool:
        """Exchange membership tables with one known node."""
        try:
            res = await self.peers.call(
                node_id,
                "gossip",
                {"members": self.m.snapshot()},
            )
        except RpcError as e:
            log.debug("gossip to %s failed: %s", node_id, e)
            return False

        self.m.merge([Entry.from_wire(d) for d in res["members"]])

        return True
