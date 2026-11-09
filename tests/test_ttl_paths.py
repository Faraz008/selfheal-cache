import asyncio

import pytest

from node.membership import State
from node.store import Record, now_ms

from .conftest import connect, eventually, sees, static_cfgs
from .test_rebalance import joiner_cfg

TTL_MS = 600_000
SCENARIOS: dict = {}


def scenario(fn):
    SCENARIOS[fn.__name__] = fn
    return fn


def deadline() -> int:
    return now_ms() + TTL_MS


@scenario
async def direct_replication(start):
    servers = [await start(c) for c in static_cfgs(3, replicas=3, write_acks=3)]
    want = deadline()
    await (await connect(servers[0].port)).cmd("SET", "k", "v", "PXAT", want)
    return servers[2], b"k", want


@scenario
async def hint_replay(start):
    cfgs = static_cfgs(4, replicas=2, write_acks=2)
    servers = [await start(c) for c in cfgs]
    key = next(
        f"k{i}".encode()
        for i in range(1000)
        if "n4" in servers[0].coord.preference_list(f"k{i}".encode())
    )
    await servers[3].stop()
    want = deadline()
    await (await connect(servers[0].port)).cmd("SET", key, "v", "PXAT", want)  # hinted
    return await start(cfgs[3]), key, want  # the revived owner receives the replay


@scenario
async def read_repair(start):
    servers = [await start(c) for c in static_cfgs(3, replicas=3, write_acks=3, read_fanout=3)]
    c = await connect(servers[0].port)
    want = deadline()
    await c.cmd("SET", "k", "v", "PXAT", want)
    servers[2].store._data.pop(b"k")  # simulate a replica that missed the write
    servers[2].store._expiring.discard(b"k")
    await c.cmd("GET", "k")  # triggers the background repair
    return servers[2], b"k", want


@scenario
async def join_stream(start):
    cfgs = static_cfgs(3, replicas=2, write_acks=2)
    servers = [await start(c) for c in cfgs]
    c = await connect(servers[0].port)
    want = deadline()
    keys = [f"k{i}".encode() for i in range(100)]
    for k in keys:
        await c.cmd("SET", k, "v", "PXAT", want)
    joiner = await start(joiner_cfg(cfgs[0].rpc_port))
    await eventually(sees(joiner, "n4", State.ALIVE), timeout=8)
    key = next(k for k in keys if "n4" in joiner.coord.preference_list(k))
    return joiner, key, want


@scenario
async def cleanup_handoff(start):
    servers = [
        await start(c)
        for c in static_cfgs(
            3, replicas=2, write_acks=2, cleanup_interval_ms=50, cleanup_delay_ms=100
        )
    ]
    key = b"stray"
    owners = servers[0].coord.preference_list(key)
    stray = next(s for s in servers if s.cfg.node_id not in owners)
    target = next(s for s in servers if s.cfg.node_id == owners[0])
    want = deadline()
    stray.store.apply(key, Record(b"v", (now_ms(), "n9"), want))  # a copy nobody should hold
    stray.rebalancer.request_cleanup()
    return target, key, want


@scenario
async def graceful_leave(start):
    servers = [await start(c) for c in static_cfgs(4, replicas=2, write_acks=2)]
    c = await connect(servers[0].port)
    want = deadline()
    keys = [f"k{i}".encode() for i in range(100)]
    for k in keys:
        await c.cmd("SET", k, "v", "PXAT", want)
    key = next(k for k in keys if "n4" in servers[0].coord.preference_list(k))
    before = set(servers[0].coord.preference_list(key))
    assert await (await connect(servers[3].port)).cmd("CLUSTER", "LEAVE") == "OK"

    async def ring_shrunk():
        return servers[0].coord.ring.nodes == ["n1", "n2", "n3"]

    await eventually(ring_shrunk, timeout=5)
    newcomer = (set(servers[0].coord.preference_list(key)) - before).pop()
    return next(s for s in servers if s.cfg.node_id == newcomer), key, want


@pytest.mark.parametrize("name", list(SCENARIOS))
async def test_expire_at_survives_every_path(name, start):
    target, key, want = await SCENARIOS[name](start)

    async def arrived():
        return target.store.get_record(key) is not None

    await eventually(arrived, timeout=8)
    rec = target.store.get_record(key)
    assert rec.expire_at == want  # bit-for-bit: never recomputed, never shifted
    assert rec.value == b"v"


async def test_expired_value_reaches_a_joiner_only_as_a_tombstone(start):
    cfgs = static_cfgs(3, replicas=3, write_acks=3)
    servers = [await start(c) for c in cfgs]
    c = await connect(servers[0].port)
    want = now_ms() + 200
    keys = [f"k{i}".encode() for i in range(40)]
    for k in keys:
        await c.cmd("SET", k, "secret", "PXAT", want)
    await asyncio.sleep(0.4)  # expired everywhere, not yet collected (grace is 10 min)
    joiner = await start(joiner_cfg(cfgs[0].rpc_port, replicas=3))
    await eventually(sees(joiner, "n4", State.ALIVE), timeout=8)
    key = next(k for k in keys if "n4" in joiner.coord.preference_list(k))
    rec = joiner.store.get_record(key)
    assert rec is not None and rec.value is None and rec.expire_at == want  # bare tombstone
    assert await (await connect(joiner.port)).cmd("GET", key) is None


def test_only_the_coordinator_path_creates_expiry_timestamps():
    """A tripwire, not a proof: no other module may build expire_at from a clock."""
    import re
    from pathlib import Path

    bad = [
        p.name
        for p in (Path(__file__).resolve().parent.parent / "node").glob("*.py")
        if p.name not in ("server.py", "coordinator.py", "store.py")
        and re.search(r"expire_at\s*=[^=\n]*(now|time)", p.read_text())
    ]
    assert not bad, bad
