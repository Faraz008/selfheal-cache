import asyncio
import time

from node.store import Record

from .conftest import connect, eventually, static_cfgs


async def test_hedged_read_bypasses_slow_replica(start):
    cfgs = static_cfgs(
        3,
        replicas=3,
        write_acks=3,
        read_fanout=1,
        hedge_ms=30,
        probe_interval_ms=60_000,
        gossip_interval_ms=60_000,
    )
    servers = [await start(c) for c in cfgs]
    n1 = servers[0]
    key = next(
        f"k{i}".encode() for i in range(500) if n1.coord.read_order(f"k{i}".encode())[0] != "n1"
    )
    c = await connect(n1.port)
    await c.cmd("SET", key, "v")
    slow_node = n1.coord.read_order(key)[0]
    real = n1.peers.call

    async def slow(node_id, op, args=None, timeout=None):
        if node_id == slow_node and op == "get":
            await asyncio.sleep(1)
        return await real(node_id, op, args, timeout)

    n1.peers.call = slow
    t0 = time.monotonic()
    assert await c.cmd("GET", key) == b"v"
    assert time.monotonic() - t0 < 0.5  # served by the hedge, not the 1 s straggler


async def test_read_survives_dead_replica_before_detection(start):
    cfgs = static_cfgs(
        3, replicas=3, write_acks=3, probe_interval_ms=60_000, gossip_interval_ms=60_000
    )
    servers = [await start(c) for c in cfgs]
    c = await connect(servers[0].port)
    await c.cmd("SET", "k", "v")
    await servers[1].stop()  # nobody has noticed yet
    for _ in range(20):
        assert await c.cmd("GET", "k") == b"v"


async def test_sloppy_quorum_write_succeeds_with_dead_replica(start):
    servers = [await start(c) for c in static_cfgs(4, replicas=2, write_acks=2)]
    n1 = servers[0]
    key = next(
        f"k{i}".encode() for i in range(1000) if "n4" in n1.coord.preference_list(f"k{i}".encode())
    )
    await servers[3].stop()  # dead, possibly not yet detected: both paths must work
    c = await connect(n1.port)
    assert await c.cmd("SET", key, "v") == "OK"  # W=2 met via a stand-in
    assert sum(s.hints.count for s in servers[:3]) == 1


async def test_write_still_works_after_detector_declares_death(start):
    from node.membership import State

    from .conftest import sees

    servers = [await start(c) for c in static_cfgs(4, replicas=2, write_acks=2)]
    await servers[3].stop()
    await eventually(sees(servers[0], "n4", State.DEAD), timeout=6)
    c = await connect(servers[0].port)
    for i in range(50):
        assert await c.cmd("SET", f"k{i}", "v") == "OK"


async def test_hint_replays_when_owner_returns(start):
    cfgs = static_cfgs(4, replicas=2, write_acks=2)
    servers = [await start(c) for c in cfgs]
    n1 = servers[0]
    key = next(
        f"k{i}".encode() for i in range(1000) if "n4" in n1.coord.preference_list(f"k{i}".encode())
    )
    await servers[3].stop()
    c = await connect(n1.port)
    assert await c.cmd("SET", key, "v") == "OK"
    assert sum(s.hints.count for s in servers[:3]) == 1

    revived = await start(cfgs[3])  # same id, same ports: a restart

    async def delivered():
        rec = revived.store.get_live(key)
        return rec is not None and rec.value == b"v"

    await eventually(delivered, timeout=8)

    async def drained():
        return sum(s.hints.count for s in servers[:3]) == 0

    await eventually(drained)


async def test_deleted_key_is_not_resurrected_by_hint_replay(start):
    cfgs = static_cfgs(4, replicas=2, write_acks=2)
    servers = [await start(c) for c in cfgs]
    n1 = servers[0]

    key = next(
        f"k{i}".encode() for i in range(1000) if "n4" in n1.coord.preference_list(f"k{i}".encode())
    )

    c = await connect(n1.port)

    try:
        await c.cmd("SET", key, "v")

        old_record = servers[3].store.get_record(key)
        assert old_record is not None
        assert old_record.value == b"v"

        await servers[3].stop()

        await c.cmd("DEL", key)

        revived = await start(cfgs[3])

        revived.store.apply(key, old_record)

        async def tombstoned():
            r = revived.store.get_record(key)
            return r is not None and r.is_tombstone

        await eventually(tombstoned, timeout=8)

        assert await c.cmd("GET", key) is None

    finally:
        c.w.close()
