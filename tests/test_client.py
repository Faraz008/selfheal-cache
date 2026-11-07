import asyncio

import pytest

from client.cluster import ClusterClient, ClusterUnavailable, RespError

from .conftest import static_cfgs


async def test_discovers_cluster_from_one_seed(start):
    servers = [await start(c) for c in static_cfgs(3)]
    async with ClusterClient([("127.0.0.1", servers[0].port)]) as cl:
        assert len(cl.nodes()) == 3
        assert await cl.set("a", "1") is True
        assert await cl.get("a") == b"1"
        assert await cl.delete("a") == 1
        assert await cl.get("a") is None


async def test_command_errors_are_raised_not_retried(start):
    servers = [await start(c) for c in static_cfgs(2)]
    async with ClusterClient([("127.0.0.1", servers[0].port)]) as cl:
        with pytest.raises(RespError):
            await cl.execute("GET")
        assert cl.stats["retries"] == 0


async def test_dead_seed_is_survived_by_retry(start):
    servers = [await start(c) for c in static_cfgs(3, replicas=3, write_acks=2)]
    async with ClusterClient([("127.0.0.1", servers[0].port)], retries=1) as cl:
        await servers[0].stop()  # the node the client knows best
        for i in range(20):
            assert await cl.set(f"k{i}", "v") is True
        assert cl.stats["failures"] == 0 and cl.stats["retries"] >= 1


async def test_unavailable_when_everything_is_down(start):
    servers = [await start(c) for c in static_cfgs(1)]
    cl = ClusterClient([("127.0.0.1", servers[0].port)], retries=1, request_timeout=0.5)
    await cl.start()
    await servers[0].stop()
    with pytest.raises(ClusterUnavailable):
        await cl.get("k")
    assert cl.stats["failures"] == 1
    await cl.close()


async def test_client_survives_node_death_mid_traffic(start):
    servers = [await start(c) for c in static_cfgs(3, replicas=3, write_acks=2)]
    acked: dict[str, str] = {}
    async with ClusterClient([("127.0.0.1", servers[0].port)], retries=1) as cl:

        async def worker(w: int):
            for i in range(120):
                k, v = f"w{w}:{i}", f"v{i}"
                await cl.set(k, v)
                acked[k] = v
                assert await cl.get(k) == v.encode()
                await asyncio.sleep(0.005)

        tasks = [asyncio.create_task(worker(w)) for w in range(8)]
        await asyncio.sleep(0.3)
        await servers[1].stop()  # kill a node mid-traffic
        await asyncio.gather(*tasks)
        assert cl.stats["failures"] == 0
        print(f"\nrequests={cl.stats['requests']} retries={cl.stats['retries']}")
        for k, v in acked.items():
            assert await cl.get(k) == v.encode()  # every acked write is readable
