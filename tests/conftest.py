import asyncio

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


@pytest.fixture
async def server():
    s = Server(NodeConfig(port=0, sweep_interval_ms=20))
    await s.start()
    yield s
    await s.stop()


@pytest.fixture
async def client(server):
    r, w = await asyncio.open_connection("127.0.0.1", server.port)
    yield Client(r, w)
    w.close()
