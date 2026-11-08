import asyncio
import hashlib

from client.cluster import ClusterClient
from node.config import NodeConfig
from node.membership import State

from .conftest import FAST, eventually, free_port, sees, static_cfgs

OVER = dict(replicas=2, write_acks=1, read_fanout=2, cleanup_interval_ms=100, cleanup_delay_ms=300)


def holders(servers, key: bytes) -> set[str]:
    return {s.cfg.node_id for s in servers if s.store.get_live(key) is not None}


async def digest(cl, keys) -> str:
    h = hashlib.sha256()
    for k in sorted(keys):
        h.update(k.encode() + b"=" + (await cl.get(k) or b"<MISSING>") + b"\n")
    return h.hexdigest()


async def test_add_node_under_load_fails_nothing_and_loses_nothing(start):
    cfgs = static_cfgs(3, **OVER)
    servers = [await start(c) for c in cfgs]
    async with ClusterClient(
        [("127.0.0.1", servers[0].port)], retries=1, request_timeout=3.0
    ) as cl:
        pre = {f"pre:{i}": f"v{i}" for i in range(1500)}
        items = list(pre.items())
        for i in range(0, len(items), 50):
            await asyncio.gather(*(cl.set(k, v) for k, v in items[i : i + 50]))
        before = await digest(cl, pre)

        acked = dict(pre)
        stop = asyncio.Event()

        async def worker(w: int):
            i = 0
            while not stop.is_set():
                k, v = f"w{w}:{i}", f"x{i}"
                i += 1
                await cl.set(k, v)
                acked[k] = v
                assert await cl.get(k) == v.encode()  # read-your-write, mid-join
                await asyncio.sleep(0.002)

        workers = [asyncio.create_task(worker(w)) for w in range(6)]
        await asyncio.sleep(0.3)
        joiner = await start(
            NodeConfig(
                node_id="n4",
                port=free_port(),
                rpc_port=free_port(),
                seeds=[f"127.0.0.1:{cfgs[0].rpc_port}"],
                **{**FAST, **OVER},
            )
        )
        for s in servers:
            await eventually(sees(s, "n4", State.ALIVE), timeout=10)
        await asyncio.sleep(0.5)
        stop.set()
        await asyncio.gather(*workers)

        assert cl.stats["failures"] == 0
        print(
            f"\nrequests={cl.stats['requests']} retries={cl.stats['retries']} "
            f"acked={len(acked)} joiner_received={joiner.rebalancer.stats['keys_received']}"
        )
        assert await digest(cl, pre) == before  # checksum of the preloaded data is unchanged
        keys = list(acked)
        for i in range(0, len(keys), 100):
            chunk = keys[i : i + 100]
            got = await asyncio.gather(*(cl.get(k) for k in chunk))
            assert [g.decode() if g else None for g in got] == [acked[k] for k in chunk]

        everyone = [*servers, joiner]

        async def placed():  # exactly N copies on exactly the right nodes: cleanup finished
            return all(
                holders(everyone, k.encode()) == set(servers[0].coord.preference_list(k.encode()))
                for k in acked
            )

        await eventually(placed, timeout=15)
