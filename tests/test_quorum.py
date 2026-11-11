import asyncio

import pytest

from .conftest import ServerError, connect, eventually, static_cfgs


def quorum(n, **over):
    base = dict(replicas=3, write_acks=2, read_quorum=2, consistency="quorum")
    return static_cfgs(n, **{**base, **over})


def holds(servers, key: bytes, value: bytes):
    async def cond():
        for s in servers:
            r = s.store.get_live(key)
            if r is None or r.value != value:
                return False
        return True

    return cond


def delay_puts(server, targets: set[str], secs: float) -> None:
    """Make one coordinator's replication to `targets` slow: a lagging replica."""
    real = server.peers.call

    async def slow(node_id, op, args=None, timeout=None):
        if op == "put" and node_id in targets:
            await asyncio.sleep(secs)
        return await real(node_id, op, args, timeout)

    server.peers.call = slow


async def test_quorum_write_fails_instead_of_going_sloppy(start):
    servers = [await start(c) for c in quorum(4, replicas=2, write_acks=2, read_quorum=1)]
    n1 = servers[0]
    key = next(f"k{i}" for i in range(1000) if "n4" in n1.coord.preference_list(f"k{i}".encode()))
    await servers[3].stop()
    c = await connect(n1.port)
    with pytest.raises(ServerError, match="CLUSTERDOWN"):
        await c.cmd("SET", key, "v")  # eventual mode would ack this via a stand-in hint
    assert sum(s.hints.count for s in servers[:3]) == 0
    assert n1.coord.stats["hints_written"] == 0


async def test_a_failed_quorum_write_is_not_rolled_back(start):
    servers = [await start(c) for c in quorum(3, write_acks=3, read_quorum=1)]
    c = await connect(servers[0].port)
    await servers[2].stop()
    with pytest.raises(ServerError, match="CLUSTERDOWN"):
        await c.cmd("SET", "k", "v")
    # The client was told "failed", but 2 of 3 replicas applied it anyway.
    assert servers[0].store.get_live(b"k") is not None
    assert servers[1].store.get_live(b"k") is not None
    assert b"quorum_write_failures:1" in await c.cmd("INFO")


async def test_quorum_survives_one_failure_of_three(start):
    servers = [await start(c) for c in quorum(3)]
    c = await connect(servers[0].port)
    await servers[2].stop()
    for i in range(50):
        assert await c.cmd("SET", f"k{i}", f"v{i}") == "OK"
    for i in range(50):
        assert await c.cmd("GET", f"k{i}") == f"v{i}".encode()


@pytest.mark.parametrize("mode", ["eventual", "quorum"])
async def test_read_with_two_of_three_nodes_down(start, mode):
    servers = [await start(c) for c in quorum(3, consistency=mode)]
    c = await connect(servers[0].port)
    await c.cmd("SET", "k", "v")
    await servers[1].stop()
    await servers[2].stop()
    if mode == "eventual":
        assert await c.cmd("GET", "k") == b"v"  # best effort: serves what it has
    else:
        with pytest.raises(ServerError, match="CLUSTERDOWN"):
            await c.cmd("GET", "k")  # refuses rather than guess
        assert b"quorum_read_failures:1" in await c.cmd("INFO")


async def test_quorum_read_repairs_before_replying(start):
    servers = [await start(c) for c in quorum(3, read_quorum=3)]  # R+W = 5 > 3
    c = await connect(servers[0].port)
    await c.cmd("SET", "k", "v")
    await eventually(holds(servers, b"k", b"v"))
    servers[2].store._remove(b"k")  # this replica lost it
    assert await c.cmd("GET", "k") == b"v"
    assert servers[2].store.get_live(b"k") is not None  # healed already: no eventually()


async def test_eventual_mode_serves_a_stale_read(start):
    servers = [await start(c) for c in static_cfgs(3, replicas=3, write_acks=1, read_fanout=1)]
    n1, n2, _ = servers
    key = next(
        f"k{i}".encode() for i in range(500) if n2.coord.read_order(f"k{i}".encode())[0] != "n1"
    )
    c1, c2 = await connect(n1.port), await connect(n2.port)
    await c1.cmd("SET", key, "old")
    await eventually(holds(servers, key, b"old"))
    delay_puts(n1, {"n2", "n3"}, 0.6)
    await c1.cmd("SET", key, "new")  # W=1: acked by n1 alone, returns at once
    assert await c2.cmd("GET", key) == b"old"  # an acked write, not yet visible here

    async def converged():
        return await c2.cmd("GET", key) == b"new"

    await eventually(converged, timeout=3)  # eventually consistent, as designed


async def test_quorum_mode_never_serves_that_stale_read(start):
    servers = [await start(c) for c in quorum(3)]
    n1, _, n3 = servers
    c1, c3 = await connect(n1.port), await connect(n3.port)
    await c1.cmd("SET", "k", "old")
    await eventually(holds(servers, b"k", b"old"))
    delay_puts(n1, {"n3"}, 1.0)  # n3 lags; n2 still acks promptly
    await c1.cmd("SET", "k", "new")  # returns once n1 + n2 have acked (W=2)
    assert n3.store.get_live(b"k").value == b"old"  # precondition: the lag is real
    for _ in range(20):
        assert await c3.cmd("GET", "k") == b"new"  # R=2 always overlaps the write quorum
