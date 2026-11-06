import asyncio
import time
from dataclasses import replace

import pytest

from node.config import NodeConfig
from node.membership import Entry, State
from node.rpc import RpcError
from node.server import Server

from .conftest import eventually, free_port

# 50 ms gossip/probes so tests finish in seconds. Defaults are 1 s.
FAST = dict(
    gossip_interval_ms=50,
    probe_interval_ms=50,
    probe_timeout_ms=200,
    suspect_timeout_ms=600,
    rpc_timeout_ms=200,
)


def static_cfgs(n: int, **over) -> list[NodeConfig]:
    ports = [(free_port(), free_port()) for _ in range(n)]
    members = [f"n{i + 1}@127.0.0.1:{p}:{r}" for i, (p, r) in enumerate(ports)]
    return [
        NodeConfig(node_id=f"n{i + 1}", port=p, rpc_port=r, members=members, **{**FAST, **over})
        for i, (p, r) in enumerate(ports)
    ]


def sees(server: Server, node_id: str, state: State):
    async def cond():
        e = server.membership.get(node_id)
        return e is not None and e.state is state

    return cond


@pytest.fixture
async def start():
    started: list[Server] = []

    async def _start(cfg: NodeConfig) -> Server:
        s = Server(cfg)
        await s.start()
        started.append(s)
        return s

    yield _start
    for s in started:
        await s.stop()


async def test_gossip_spreads_new_member_and_updates_ring(start):
    servers = [await start(c) for c in static_cfgs(3, probe_interval_ms=60_000)]
    servers[0].membership.merge([Entry("n9", "127.0.0.1", 1, 2, State.ALIVE, 5)])

    async def everywhere():
        return all(
            s.membership.get("n9") is not None and "n9" in s.coord.ring.nodes for s in servers
        )

    await eventually(everywhere, timeout=3)
