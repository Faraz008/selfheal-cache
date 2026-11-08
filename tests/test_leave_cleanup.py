import asyncio

import pytest

from node.membership import State
from node.store import Record, now_ms

from .conftest import ServerError, connect, eventually, sees, static_cfgs


def holders(servers, key: bytes) -> set[str]:
    return {s.cfg.node_id for s in servers if s.store.get_live(key) is not None}


async def test_graceful_leave_hands_data_to_new_owners(start):
    servers = [await start(c) for c in static_cfgs(4, replicas=2, write_acks=2)]
    c = await connect(servers[0].port)
    keys = [f"k{i}".encode() for i in range(300)]
    for k in keys:
        await c.cmd("SET", k, "v")

    leaver, rest = servers[3], servers[:3]
    assert await (await connect(leaver.port)).cmd("CLUSTER", "LEAVE") == "OK"
    assert leaver.membership.self_entry.state is State.LEFT
    assert leaver.store.raw_len() == 0  # everything handed off and dropped
    for s in rest:
        await eventually(sees(s, "n4", State.LEFT), timeout=5)
        assert s.coord.ring.nodes == ["n1", "n2", "n3"]

    async def placed():
        return all(holders(rest, k) == set(rest[0].coord.preference_list(k)) for k in keys)

    await eventually(placed, timeout=5)  # exactly N copies, on exactly the right nodes
    assert await c.cmd("GET", "k7") == b"v"


async def test_writes_during_leave_also_reach_future_owners(start):
    servers = [await start(c) for c in static_cfgs(4, replicas=2, write_acks=2)]
    n1 = servers[0]
    servers[3].membership.begin_leave()  # announce only; no drain

    async def spread():
        return all(s.coord.ring_after is not None for s in servers[:3])

    await eventually(spread)
    key = next(
        k for k in (f"k{i}".encode() for i in range(1000)) if "n4" in n1.coord.preference_list(k)
    )
    pref = n1.coord.preference_list(key)
    future = [n for n in n1.coord.ring_after.get_preference_list(key, 2) if n not in pref]
    assert len(future) == 1
    target = next(s for s in servers if s.cfg.node_id == future[0])
    await (await connect(n1.port)).cmd("SET", key, "v")

    async def landed():
        return target.store.get_live(key) is not None

    await eventually(landed)


async def test_leave_refused_for_a_single_node(start):
    s = await start(static_cfgs(1)[0])
    with pytest.raises(ServerError, match="only node"):
        await (await connect(s.port)).cmd("CLUSTER", "LEAVE")


async def test_forget_removes_a_dead_node_from_the_ring(start):
    servers = [await start(c) for c in static_cfgs(3, replicas=2)]
    await servers[2].stop()
    await eventually(sees(servers[0], "n3", State.DEAD), timeout=6)
    c = await connect(servers[0].port)
    assert await c.cmd("CLUSTER", "FORGET", "n3") == "OK"
    await eventually(sees(servers[1], "n3", State.LEFT), timeout=4)
    assert servers[0].coord.ring.nodes == servers[1].coord.ring.nodes == ["n1", "n2"]
    with pytest.raises(ServerError, match="not dead"):
        await c.cmd("CLUSTER", "FORGET", "n2")


CLEAN = dict(replicas=2, write_acks=2, cleanup_interval_ms=50, cleanup_delay_ms=100)


async def test_cleanup_hands_off_then_drops(start):
    servers = [await start(c) for c in static_cfgs(3, **CLEAN)]
    key = b"stray"
    owners = servers[0].coord.preference_list(key)
    stray = next(s for s in servers if s.cfg.node_id not in owners)
    stray.store.apply(key, Record(b"v", (now_ms(), "n9")))  # a copy nobody should hold
    stray.rebalancer.request_cleanup()

    async def moved():
        owners_have = all(
            s.store.get_live(key) is not None for s in servers if s.cfg.node_id in owners
        )
        return owners_have and stray.store.get_record(key) is None

    await eventually(moved, timeout=4)


async def test_cleanup_keeps_the_record_while_an_owner_is_unreachable(start):
    servers = [await start(c) for c in static_cfgs(3, **CLEAN)]
    key = b"stray"
    owners = servers[0].coord.preference_list(key)
    stray = next(s for s in servers if s.cfg.node_id not in owners)
    down = next(s for s in servers if s.cfg.node_id == owners[1])
    await down.stop()
    stray.store.apply(key, Record(b"v", (now_ms(), "n9")))
    stray.rebalancer.request_cleanup()
    await asyncio.sleep(1.0)  # many cleanup rounds
    assert stray.store.get_record(key) is not None  # never deleted without confirmation
