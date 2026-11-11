from dataclasses import replace

from node.evict import OVERHEAD

from .conftest import connect, static_cfgs


async def test_cap_is_local_and_data_stays_readable(start):
    cfgs = static_cfgs(3, replicas=3, write_acks=3, read_fanout=2)
    cap = 40 * (len(b"key:000") + 100 + OVERHEAD)  # room for ~40 records
    cfgs[2] = replace(cfgs[2], max_memory_bytes=cap)  # only n3 is capped
    servers = [await start(c) for c in cfgs]
    c = await connect(servers[0].port)
    for i in range(200):
        await c.cmd("SET", f"key:{i:03d}", "x" * 100)

    n3 = servers[2]
    assert n3.store.mem <= cap and len(n3.store) <= 40
    assert n3.store.evictions >= 150
    for s in servers[:2]:
        assert len(s.store) == 200  # eviction on n3 never touched anyone else
    assert all(s.hints.count == 0 for s in servers)  # no hint storm
    assert all(s.store.raw_len() == 200 for s in servers[:2])  # and no tombstones written

    for i in range(200):  # n3 misses, but a read that consults another replica still hits
        assert await c.cmd("GET", f"key:{i:03d}") == b"x" * 100
    assert n3.store.mem <= cap  # read repair refilling n3 respects the cap too


async def test_info_reports_memory(start):
    cfg = replace(static_cfgs(1)[0], max_memory_bytes=10_000)
    s = await start(cfg)
    out = await (await connect(s.port)).cmd("INFO")
    assert b"max_memory:10000" in out and b"eviction_policy:lru" in out
    assert b"used_memory:0" in out and b"evictions:0" in out
