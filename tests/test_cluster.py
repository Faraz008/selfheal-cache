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


async def test_write_fails_cleanly_when_acks_unreachable(make):
    # Documents TODAY's behavior. Phase 5 (sloppy quorum + hints) fixes it.
    servers = await make(3, replicas=2, write_acks=2, rpc_timeout_ms=300)
    c1 = await connect(servers[0].port)
    key = next(
        f"k{i}" for i in range(1000) if "n3" in servers[0].coord.preference_list(f"k{i}".encode())
    )
    await servers[2].stop()
    with pytest.raises(ServerError, match="CLUSTERDOWN"):
        await c1.cmd("SET", key, "v")
