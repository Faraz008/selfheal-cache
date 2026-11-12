import pytest

from node.clock import hlc_ms
from node.store import now_ms

from .conftest import SkewClock, connect, eventually, static_cfgs


async def skewed_cluster(start, offsets_ms: list[int]):
    n = len(offsets_ms)
    cfgs = static_cfgs(n, replicas=n, write_acks=n)
    return [await start(c, SkewClock(o)) for c, o in zip(cfgs, offsets_ms, strict=True)]


@pytest.mark.parametrize("hlc", [True, False], ids=["hlc-on", "control-no-hlc"])
async def test_a_later_write_through_a_slow_clock_node_still_wins(start, hlc):
    servers = await skewed_cluster(start, [+2000, -2000, 0])
    fast, slow = servers[0], servers[1]
    if not hlc:  # negative control: versions never learn from each other
        for s in servers:
            s.coord.versions.observe = lambda ts: True
    cf, cs = await connect(fast.port), await connect(slow.port)
    await cf.cmd("SET", "k", "first")  # stamped by the node whose clock is 2 s fast
    await cs.cmd("SET", "k", "second")  # strictly later in real time, stamped by the 2 s slow one
    expected = b"second" if hlc else b"first"  # the control shows the bug the HLC prevents
    assert await cs.cmd("GET", "k") == expected
    assert {s.store.get_live(b"k").value for s in servers} == {expected}


async def test_gossip_pulls_a_slow_clock_forward(start):
    fast, slow, _ = await skewed_cluster(start, [+2000, -2000, 0])
    stamp = fast.coord.versions.next()[0]  # note: no write touches the slow node

    async def caught_up():
        return slow.coord.versions.last >= stamp

    await eventually(caught_up, timeout=3)


async def test_an_absurd_clock_is_refused_not_adopted(start):
    good, other, crazy = await skewed_cluster(start, [0, 0, +60_000])  # limit is 5 s
    await (await connect(crazy.port)).cmd("SET", "k", "v")  # stamps a version a minute ahead

    async def refused():
        return good.coord.versions.rejected > 0

    await eventually(refused, timeout=3)
    assert hlc_ms(good.coord.versions.last) - now_ms() < 5000


async def test_ttl_expiry_is_consistent_only_to_within_the_clock_skew(start):
    fast, slow, mid = await skewed_cluster(start, [+1000, -1000, 0])
    cf, cs = await connect(fast.port), await connect(slow.port)
    want = now_ms() + 300
    await cs.cmd("SET", "k", "v", "PXAT", want)
    # every replica holds the SAME absolute deadline...
    assert {s.store.get_record(b"k").expire_at for s in (fast, slow, mid)} == {want}
    # ...but each judges it by its own clock: inside the window they disagree
    assert await cf.cmd("GET", "k") is None  # +1 s clock: long expired
    assert await cs.cmd("GET", "k") == b"v"  # -1 s clock: still serving it

    async def slow_agrees():
        return await cs.cmd("GET", "k") is None

    await eventually(slow_agrees, timeout=3)
    lag = now_ms() - want
    print(f"\nslow-clock node stopped serving {lag} ms after the deadline")
    assert 950 <= lag <= 1800  # bounded by the skew, as documented


async def test_restarted_replica_keeps_the_original_deadline(start):
    cfgs = static_cfgs(3, replicas=3, write_acks=3, sync_on_start=True)
    servers = [await start(c) for c in cfgs]
    want = now_ms() + 2500
    await (await connect(servers[0].port)).cmd("SET", "k", "v", "PXAT", want)
    await servers[2].stop()  # killed mid-TTL
    reborn = await start(cfgs[2])  # comes back EMPTY, catches up from peers

    async def refilled():
        return reborn.store.get_record(b"k") is not None

    await eventually(refilled, timeout=2)
    assert reborn.store.get_record(b"k").expire_at == want  # not "now + remaining"

    async def gone_everywhere():
        return all(s.store.get_live(b"k") is None for s in (servers[0], servers[1], reborn))

    await eventually(gone_everywhere, timeout=4)
    assert now_ms() >= want
    assert await (await connect(reborn.port)).cmd("GET", "k") is None
