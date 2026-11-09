import asyncio

from node.store import now_ms

from .conftest import connect, eventually, static_cfgs
from .test_rebalance import joiner_cfg


async def test_expire_and_persist_replicate_to_every_replica(start):
    servers = [await start(c) for c in static_cfgs(3, replicas=3, write_acks=3)]
    c0, c1, _ = [await connect(s.port) for s in servers]
    await c0.cmd("SET", "k", "v")
    assert await c1.cmd("EXPIRE", "k", 100) == 1  # via a DIFFERENT coordinator
    stamps = {s.store.get_record(b"k").expire_at for s in servers}
    assert len(stamps) == 1 and None not in stamps
    assert await c0.cmd("PERSIST", "k") == 1
    assert {s.store.get_record(b"k").expire_at for s in servers} == {None}
    assert await c1.cmd("TTL", "k") == -1


async def test_expire_in_the_past_deletes_the_key_everywhere(start):
    servers = [await start(c) for c in static_cfgs(3, replicas=3, write_acks=3)]
    c = await connect(servers[0].port)
    await c.cmd("SET", "k", "v")
    assert await c.cmd("EXPIRE", "k", -1) == 1
    for s in servers:
        assert s.store.get_live(b"k") is None
        assert s.store.get_record(b"k").is_tombstone
    assert await c.cmd("PEXPIREAT", "k", 1) == 0  # already gone


async def test_ttl_updates_during_a_join_land_on_every_owner(start):
    cfgs = static_cfgs(3, replicas=2, write_acks=2)
    servers = [await start(c) for c in cfgs]
    c = await connect(servers[0].port)
    keys = [f"k{i}".encode() for i in range(200)]
    for k in keys:
        await c.cmd("SET", k, "v")
    last: dict[bytes, int] = {}

    async def churn():
        for rnd in range(1, 6):
            for k in keys:
                ts = now_ms() + 600_000 + rnd * 1000
                await c.cmd("PEXPIREAT", k, ts)
                last[k] = ts

    task = asyncio.create_task(churn())
    joiner = await joiner_started(start, cfgs)
    await task
    everyone = [*servers, joiner]

    async def consistent():
        for k in keys:
            for s in everyone:
                if s.cfg.node_id in joiner.coord.preference_list(k):
                    r = s.store.get_record(k)
                    if r is None or r.expire_at != last[k]:
                        return False
        return True

    await eventually(consistent, timeout=10)  # every owner holds the LAST deadline


async def joiner_started(start, cfgs):
    from node.membership import State

    from .conftest import sees

    joiner = await start(joiner_cfg(cfgs[0].rpc_port))
    await eventually(sees(joiner, "n4", State.ALIVE), timeout=10)
    return joiner
