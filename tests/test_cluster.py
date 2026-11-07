import asyncio

import pytest

from node.config import NodeConfig
from node.server import Server

from .conftest import ServerError, connect, eventually, free_port


@pytest.fixture
async def make():
    started: list[Server] = []

    async def _make(n=3, **over) -> list[Server]:
        ports = [(free_port(), free_port()) for _ in range(n)]
        members = [f"n{i + 1}@127.0.0.1:{p}:{r}" for i, (p, r) in enumerate(ports)]
        servers = []
        for i, (p, r) in enumerate(ports):
            cfg = NodeConfig(node_id=f"n{i + 1}", port=p, rpc_port=r, members=members, **over)
            s = Server(cfg)
            await s.start()
            servers.append(s)
            started.append(s)
        return servers

    yield _make
    for s in started:
        await s.stop()  # stop() is idempotent, so tests may stop nodes themselves


def holders(servers, key: bytes) -> set[str]:
    return {s.cfg.node_id for s in servers if s.store.get_live(key) is not None}


async def test_write_via_any_node_read_via_any_node(make):
    servers = await make(3, replicas=2, write_acks=2)
    clients = [await connect(s.port) for s in servers]
    for i, writer in enumerate(clients):
        assert await writer.cmd("SET", f"k{i}", f"v{i}") == "OK"
        for reader in clients:
            assert await reader.cmd("GET", f"k{i}") == f"v{i}".encode()


async def test_every_key_lives_on_exactly_its_preference_list(make):
    servers = await make(3, replicas=2, write_acks=2)
    clients = [await connect(s.port) for s in servers]
    for i in range(200):
        await clients[i % 3].cmd("SET", f"key:{i}", "x")
    for i in range(200):
        k = f"key:{i}".encode()
        assert holders(servers, k) == set(servers[0].coord.preference_list(k))
        assert len(holders(servers, k)) == 2


async def test_all_nodes_agree_on_placement(make):
    servers = await make(3, replicas=2)
    clients = [await connect(s.port) for s in servers]
    for i in range(50):
        answers = [await c.cmd("WHERE", f"key:{i}") for c in clients]
        assert answers[0] == answers[1] == answers[2]
        assert len(answers[0]) == 2


async def test_async_replication_converges_with_w1(make):
    servers = await make(3, replicas=3, write_acks=1)
    c = await connect(servers[0].port)
    await c.cmd("SET", "k", "v")

    async def everywhere():
        return len(holders(servers, b"k")) == 3

    await eventually(everywhere)


async def test_read_fails_over_when_primary_is_down(make):
    servers = await make(3, replicas=2, write_acks=2, rpc_timeout_ms=300)
    c1 = await connect(servers[0].port)
    key = next(
        f"k{i}"
        for i in range(1000)
        if servers[0].coord.preference_list(f"k{i}".encode())[0] == "n3"
    )
    await c1.cmd("SET", key, "v")
    await servers[2].stop()  # kill the primary
    assert await c1.cmd("GET", key) == b"v"  # served by the second replica


async def test_write_fails_cleanly_when_no_standin_exists(make):
    servers = await make(2, replicas=2, write_acks=2, rpc_timeout_ms=300)
    c1 = await connect(servers[0].port)
    await servers[1].stop()
    with pytest.raises(ServerError, match="CLUSTERDOWN"):
        await c1.cmd("SET", "k", "v")  # no third node to hold a hint


async def test_delete_replicates_as_tombstone(make):
    servers = await make(3, replicas=2, write_acks=2)
    c0, c1, c2 = [await connect(s.port) for s in servers]
    await c0.cmd("SET", "k", "v")
    assert await c1.cmd("DEL", "k") == 1
    for c in (c0, c1, c2):
        assert await c.cmd("GET", "k") is None
    for s in servers:
        if s.cfg.node_id in servers[0].coord.preference_list(b"k"):
            assert s.store.get_record(b"k").is_tombstone


async def test_concurrent_writes_converge_to_one_value(make):
    servers = await make(3, replicas=3, write_acks=3)
    clients = [await connect(s.port) for s in servers]
    for rnd in range(20):
        key = f"k{rnd}"
        await asyncio.gather(*(c.cmd("SET", key, f"from-{i}") for i, c in enumerate(clients)))
        values = {s.store.get_live(key.encode()).value for s in servers}
        assert len(values) == 1, values  # all replicas picked the same winner


async def test_ttl_is_one_absolute_timestamp_on_every_replica(make):
    servers = await make(3, replicas=3, write_acks=3)
    c = await connect(servers[0].port)
    await c.cmd("SET", "k", "v", "EX", 100)
    stamps = {s.store.get_record(b"k").expire_at for s in servers}
    assert len(stamps) == 1 and None not in stamps  # Phase 7 groundwork, free


async def test_overwrite_after_delete_wins(make):
    servers = await make(3, replicas=3, write_acks=3)
    c0, c1, _ = [await connect(s.port) for s in servers]
    await c0.cmd("SET", "k", "1")
    await c1.cmd("DEL", "k")
    await c0.cmd("SET", "k", "2")
    for s in servers:
        assert s.store.get_live(b"k").value == b"2"
