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
