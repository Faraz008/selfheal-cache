import asyncio
import socket
import time

import pytest

from node import resp
from node.config import NodeConfig
from node.server import Server


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
    s = Server(NodeConfig(port=0, rpc_port=0, sweep_interval_ms=20))
    await s.start()
    yield s
    await s.stop()


@pytest.fixture
async def client(server):
    c = await connect(server.port)
    yield c
    c.w.close()
