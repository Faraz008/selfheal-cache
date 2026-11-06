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


async def test_kill_is_detected_by_every_node(start):
    servers = [await start(c) for c in static_cfgs(5)]
    victim = servers[2]
    survivors = [s for s in servers if s is not victim]
    t0 = time.monotonic()
    await victim.stop()  # no goodbye: indistinguishable from a crash

    async def all_dead():
        return all(s.membership.get("n3").state is State.DEAD for s in survivors)

    await eventually(all_dead, timeout=6)
    print(f"\nconvergence: {time.monotonic() - t0:.2f}s")
    for s in survivors:
        for nid in ("n1", "n2", "n4", "n5"):
            assert s.membership.get(nid).state is not State.DEAD


async def test_indirect_probe_prevents_false_suspicion(start):
    servers = [await start(c) for c in static_cfgs(3)]
    n1 = servers[0]
    real = n1.peers.call

    async def broken(node_id, op, args=None, timeout=None):
        if node_id == "n3" and op in ("ping", "gossip"):
            raise RpcError("simulated broken path n1->n3")
        return await real(node_id, op, args, timeout)

    n1.peers.call = broken  # only n1's path to n3 is cut; n3 itself is fine
    await asyncio.sleep(1.5)  # ~20 probe rounds
    assert n1.membership.get("n3").state is State.ALIVE


async def test_seed_bootstrap_spreads_membership(start):
    ports = [(free_port(), free_port()) for _ in range(3)]
    n1 = await start(NodeConfig(node_id="n1", port=ports[0][0], rpc_port=ports[0][1], **FAST))
    seed = [f"127.0.0.1:{ports[0][1]}"]  # every other node knows ONLY this
    others = [
        await start(
            NodeConfig(
                node_id=f"n{i + 2}",
                port=ports[i + 1][0],
                rpc_port=ports[i + 1][1],
                seeds=seed,
                **FAST,
            )
        )
        for i in range(2)
    ]
    servers = [n1, *others]

    async def converged():
        return all(
            len(s.membership.entries()) == 3
            and all(e.state is State.ALIVE for e in s.membership.entries())
            for s in servers
        )

    await eventually(converged, timeout=5)
    assert len({tuple(s.coord.ring.nodes) for s in servers}) == 1  # identical rings
    assert servers[0].coord.ring.nodes == ["n1", "n2", "n3"]


async def test_bootstrap_retries_until_seed_appears(start):
    ports = [(free_port(), free_port()) for _ in range(2)]
    late = await start(
        NodeConfig(
            node_id="n2",
            port=ports[1][0],
            rpc_port=ports[1][1],
            seeds=[f"127.0.0.1:{ports[0][1]}"],
            **FAST,
        )
    )
    assert late.membership.self_entry.state is State.JOINING
    await asyncio.sleep(0.5)  # seed still down; node keeps trying
    n1 = await start(NodeConfig(node_id="n1", port=ports[0][0], rpc_port=ports[0][1], **FAST))
    await eventually(sees(late, "n2", State.ALIVE), timeout=8)
    await eventually(sees(n1, "n2", State.ALIVE), timeout=8)


async def test_false_suspicion_is_refuted(start):
    servers = [await start(c) for c in static_cfgs(3, suspect_timeout_ms=3000)]
    n1, n2 = servers[0], servers[1]
    await eventually(sees(n1, "n2", State.ALIVE))
    before = n1.membership.get("n2").incarnation
    assert n1.membership.mark_suspect("n2")  # n2 is perfectly healthy

    async def cleared():
        e = n1.membership.get("n2")
        return e.state is State.ALIVE and e.incarnation > before

    await eventually(cleared, timeout=3)
    assert n2.membership.self_entry.incarnation > before  # n2 bumped it itself


async def test_returning_node_is_alive_again(start):
    cfgs = static_cfgs(3)
    servers = [await start(c) for c in cfgs]
    await servers[2].stop()
    await eventually(sees(servers[0], "n3", State.DEAD), timeout=6)
    await eventually(sees(servers[1], "n3", State.DEAD), timeout=6)
    dead_inc = servers[0].membership.get("n3").incarnation

    await start(cfgs[2])  # same id, same ports: a restart
    await eventually(sees(servers[0], "n3", State.ALIVE), timeout=6)
    await eventually(sees(servers[1], "n3", State.ALIVE), timeout=6)
    assert servers[0].membership.get("n3").incarnation > dead_inc


async def test_read_order_puts_dead_and_joining_last(start):
    cfg = static_cfgs(3, replicas=3, probe_interval_ms=60_000, gossip_interval_ms=60_000)[0]
    s = await start(cfg)  # only n1 runs; its view of n2/n3 is what we test
    first = s.coord.read_order(b"k")[0]
    s.membership.merge([replace(s.membership.get(first), state=State.DEAD, incarnation=10)])
    order = s.coord.read_order(b"k")
    assert order[-1] == first and sorted(order) == ["n1", "n2", "n3"]
    s.membership.merge([replace(s.membership.get(order[0]), state=State.JOINING, incarnation=10)])
    assert s.coord.read_order(b"k")[-1] == first  # DEAD still behind JOINING
