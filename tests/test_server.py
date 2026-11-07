import asyncio

import pytest

from node import resp
from node.config import NodeConfig
from node.server import Server

from .conftest import ServerError, connect


async def test_ping(client):
    assert await client.cmd("PING") == "PONG"
    assert await client.cmd("PING", "hi") == b"hi"


async def test_set_get_del(client):
    assert await client.cmd("SET", "a", "1") == "OK"
    assert await client.cmd("GET", "a") == b"1"
    assert await client.cmd("DEL", "a", "b") == 1
    assert await client.cmd("GET", "a") is None


async def test_px_expiry(client):
    await client.cmd("SET", "a", "1", "PX", 100)
    assert 0 < await client.cmd("PTTL", "a") <= 100
    await asyncio.sleep(0.2)
    assert await client.cmd("GET", "a") is None
    assert await client.cmd("TTL", "a") == -2


async def test_nx_xx(client):
    assert await client.cmd("SET", "a", "1", "NX") == "OK"
    assert await client.cmd("SET", "a", "2", "NX") is None
    assert await client.cmd("SET", "z", "2", "XX") is None
    assert await client.cmd("SET", "a", "3", "XX") == "OK"
    assert await client.cmd("GET", "a") == b"3"


async def test_expire_persist_ttl(client):
    await client.cmd("SET", "a", "1")
    assert await client.cmd("TTL", "a") == -1
    assert await client.cmd("EXPIRE", "a", 10) == 1
    assert await client.cmd("TTL", "a") in (9, 10)
    assert await client.cmd("PERSIST", "a") == 1
    assert await client.cmd("TTL", "a") == -1
    assert await client.cmd("EXPIRE", "missing", 10) == 0


async def test_errors(client):
    with pytest.raises(ServerError, match="unknown command"):
        await client.cmd("NOPE")
    with pytest.raises(ServerError, match="wrong number"):
        await client.cmd("GET")
    with pytest.raises(ServerError, match="not an integer"):
        await client.cmd("EXPIRE", "a", "xyz")
    with pytest.raises(ServerError, match="syntax"):
        await client.cmd("SET", "a", "1", "BOGUS")


async def test_pipelining(client):
    client.w.write(resp.encode_command("SET", "a", "1") + resp.encode_command("GET", "a"))
    assert await client.read_reply() == "OK"
    assert await client.read_reply() == b"1"


async def test_sweeper_collects_without_reads():
    s = Server(
        NodeConfig(
            port=0,
            rpc_port=0,
            sweep_interval_ms=20,
            tombstone_grace_ms=0,
            hint_ttl_ms=0,
        )
    )
    await s.start()
    c = await connect(s.port)
    try:
        await c.cmd("SET", "a", "1", "PX", 30)
        await asyncio.sleep(0.3)
        assert s.store.raw_len() == 0  # physically gone, nobody read it
    finally:
        c.w.close()
        await s.stop()


async def test_large_value(client):
    big = b"x" * 1_000_000
    await client.cmd("SET", "big", big)
    assert await client.cmd("GET", "big") == big


async def test_cluster_nodes_command(client):
    out = await client.cmd("CLUSTER", "NODES")
    assert b"myself" in out and b"alive" in out
