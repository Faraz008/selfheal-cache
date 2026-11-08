import time
from dataclasses import replace

import pytest

from node.config import NodeConfig
from node.membership import State
from node.rebalance import Pacer
from node.ring import hash64
from node.rpc import RpcError
from node.wire import b64d

from .conftest import (
    FAST,
    connect,
    eventually,
    free_port,
    sees,
    static_cfgs,
)


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


def joiner_cfg(seed_rpc_port: int, node_id: str = "n4", **over) -> NodeConfig:
    return NodeConfig(
        node_id=node_id,
        port=free_port(),
        rpc_port=free_port(),
        seeds=[f"127.0.0.1:{seed_rpc_port}"],
        **{**FAST, "replicas": 2, **over},
    )


async def test_join_pulls_its_ranges_before_going_alive(start):
    cfgs = static_cfgs(3, replicas=2, write_acks=2)
    servers = [await start(c) for c in cfgs]
    c = await connect(servers[0].port)
    keys = [f"k{i}".encode() for i in range(300)]
    for k in keys:
        await c.cmd("SET", k, "v")

    joiner = await start(joiner_cfg(cfgs[0].rpc_port))
    held_when_alive: list[int] = []

    def on_change(changed):
        for e in changed:
            if e.node_id == "n4" and e.state is State.ALIVE:
                held_when_alive.append(joiner.store.raw_len())

    joiner.membership.subscribe(on_change)
    await eventually(sees(joiner, "n4", State.ALIVE), timeout=8)

    mine = [k for k in keys if "n4" in joiner.coord.preference_list(k)]
    assert len(mine) > 50  # about half of all keys with N=2 on 4 nodes
    assert held_when_alive and held_when_alive[0] >= len(mine)  # data first, ALIVE second
    assert all(joiner.store.get_live(k) is not None for k in mine)


async def test_restarted_node_catches_up_from_peers(start):
    cfgs = static_cfgs(3, replicas=3, write_acks=3, sync_on_start=True)
    servers = [await start(c) for c in cfgs]
    c = await connect(servers[0].port)
    for i in range(200):
        await c.cmd("SET", f"k{i}", "v")
    await servers[2].stop()
    reborn = await start(cfgs[2])  # same id, same ports, empty store

    async def refilled():
        return len(reborn.store) == 200

    await eventually(refilled, timeout=8)


async def test_read_plan_widens_while_a_replica_is_joining(start):
    cfg = static_cfgs(3, replicas=2, probe_interval_ms=60_000, gossip_interval_ms=60_000)[0]
    s = await start(cfg)
    key = b"k"
    pref = s.coord.preference_list(key)
    order, joining = s.coord.read_plan(key)
    assert joining == 0 and sorted(order) == sorted(pref)

    joiner = next(n for n in pref if n != "n1")
    s.membership.merge([replace(s.membership.get(joiner), state=State.JOINING, incarnation=10)])
    order, joining = s.coord.read_plan(key)
    assert joining == 1 and len(order) == 3
    assert order[-1] == joiner  # asked last...
    assert set(order) == {"n1", "n2", "n3"}  # ...but the displaced old replica is now included
