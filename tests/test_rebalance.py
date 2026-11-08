import time

import pytest

from node.rebalance import Pacer
from node.ring import hash64
from node.rpc import RpcError
from node.wire import b64d

from .conftest import connect, static_cfgs


async def test_pacer_limits_average_rate():
    p = Pacer(1000)
    t0 = time.monotonic()
    await p.take(100)
    await p.take(100)
    assert time.monotonic() - t0 >= 0.19  # 200 records at 1000/s


async def test_pacer_unlimited_returns_immediately():
    t0 = time.monotonic()
    await Pacer(0).take(1_000_000)
    assert time.monotonic() - t0 < 0.05


async def test_stream_session_transfers_only_requested_ranges(start):
    cfgs = static_cfgs(
        2, replicas=2, write_acks=2, probe_interval_ms=60_000, gossip_interval_ms=60_000
    )
    a, b = [await start(c) for c in cfgs]
    c = await connect(a.port)
    for i in range(300):
        await c.cmd("SET", f"k{i}", "v")
    half = 1 << 63
    expected = sorted(k for k in (f"k{i}".encode() for i in range(300)) if hash64(k) < half)
    opened = await b.peers.call("n1", "stream_open", {"ranges": [[0, half]]}, timeout=5)
    assert opened["total"] == len(expected)
    got = []
    while True:
        r = await b.peers.call("n1", "stream_next", {"sid": opened["sid"], "n": 50}, timeout=5)
        got += [b64d(i["k"]) for i in r["items"]]
        if r["done"]:
            break
    assert sorted(got) == expected
    with pytest.raises(RpcError, match="unknown or expired"):
        await b.peers.call("n1", "stream_next", {"sid": opened["sid"], "n": 1})
