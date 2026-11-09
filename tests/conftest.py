import asyncio
import socket
import time

import pytest

from node import resp
from node.config import NodeConfig
from node.membership import State
from node.server import Server
from node.store import Store, now_ms


class SkewClock:
    """Wall clock with an adjustable offset: lets one node run fast or slow."""

    def __init__(self, offset_ms: int = 0):
        self.offset = offset_ms

    def __call__(self) -> int:
        return now_ms() + self.offset


class ServerError(Exception):
    pass


class Client:
    def __init__(self, r, w):
        self.r, self.w = r, w

    async def read_reply(self):
        line = (await self.r.readline())[:-2]
        t, rest = line[:1], line[1:]

        if t == b"+":
            return rest.decode()

        if t == b"-":
            raise ServerError(rest.decode())

        if t == b":":
            return int(rest)

        if t == b"$":
            n = int(rest)
            if n == -1:
                return None
            return (await self.r.readexactly(n + 2))[:-2]

        if t == b"*":
            return [await self.read_reply() for _ in range(int(rest))]

        raise AssertionError(f"bad reply {line!r}")

    async def cmd(self, *parts):
        self.w.write(resp.encode_command(*parts))
        return await self.read_reply()


async def connect(port: int) -> Client:
    r, w = await asyncio.open_connection("127.0.0.1", port)
    return Client(r, w)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def eventually(cond, timeout: float = 3.0) -> None:
    """Poll an async predicate. Eventual consistency needs a deadline, not a sleep."""
    end = time.monotonic() + timeout

    while time.monotonic() < end:
        if await cond():
            return
        await asyncio.sleep(0.02)

    raise AssertionError("condition not met in time")


@pytest.fixture
async def server():
    s = Server(
        NodeConfig(
            port=0,
            rpc_port=0,
            sweep_interval_ms=20,
        )
    )
    await s.start()
    yield s
    await s.stop()


@pytest.fixture
async def client(server):
    c = await connect(server.port)
    yield c
    c.w.close()


# ---- Multi-node cluster test helpers ----

FAST = dict(
    gossip_interval_ms=50,
    probe_interval_ms=50,
    probe_timeout_ms=200,
    suspect_timeout_ms=600,
    rpc_timeout_ms=200,
    hint_replay_interval_ms=100,
    hedge_ms=30,
)

FAST.update(
    rebalance_settle_ms=150,
    transfer_rate=0,
    join_max_wait_ms=8000,
    leave_timeout_ms=10_000,
    sync_on_start=False,
)


def static_cfgs(n: int, **over) -> list[NodeConfig]:
    ports = [(free_port(), free_port()) for _ in range(n)]

    members = [f"n{i + 1}@127.0.0.1:{p}:{r}" for i, (p, r) in enumerate(ports)]

    return [
        NodeConfig(
            node_id=f"n{i + 1}",
            port=p,
            rpc_port=r,
            members=members,
            **{**FAST, **over},
        )
        for i, (p, r) in enumerate(ports)
    ]


def sees(server: Server, node_id: str, state: State):
    async def cond():
        e = server.membership.get(node_id)
        return e is not None and e.state is state

    return cond


@pytest.fixture
async def start():
    started: list[Server] = []

    async def _start(cfg: NodeConfig, clock=None) -> Server:
        store = Store(clock, cfg.tombstone_grace_ms) if clock is not None else None
        s = Server(cfg, store)
        await s.start()
        started.append(s)
        return s

    yield _start
    for s in started:
        await s.stop()
