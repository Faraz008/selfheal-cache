import asyncio
import itertools
import json
import logging
import struct
import time
from collections.abc import Awaitable, Callable
from typing import Any

log = logging.getLogger("rpc")

MAX_FRAME = 32 * 1024 * 1024
_HDR = struct.Struct(">I")
_NET_ERRORS = (asyncio.IncompleteReadError, OSError, ValueError)  # ValueError: bad JSON


class RpcError(Exception):
    """Any failure talking to a peer: connect, timeout, disconnect, remote error."""


def encode_frame(msg: dict) -> bytes:
    body = json.dumps(msg, separators=(",", ":")).encode()
    if len(body) > MAX_FRAME:
        raise RpcError("message too large")
    return _HDR.pack(len(body)) + body


async def read_frame(reader: asyncio.StreamReader) -> dict | None:
    """Next message, or None on clean EOF between frames."""
    try:
        hdr = await reader.readexactly(4)
    except asyncio.IncompleteReadError as e:
        if not e.partial:
            return None
        raise
    (n,) = _HDR.unpack(hdr)
    if n > MAX_FRAME:
        raise RpcError(f"frame too large: {n}")
    return json.loads(await reader.readexactly(n))


# ---------------------------------------------------------------- server
Handler = Callable[[dict], Awaitable[Any]]


class RpcServer:
    def __init__(self, host: str, port: int, handlers: dict[str, Handler]):
        self.host, self._port, self.handlers = host, port, handlers
        self._server: asyncio.Server | None = None
        self._writers: set[asyncio.StreamWriter] = set()

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._on_conn, self.host, self._port)

    @property
    def port(self) -> int:
        return self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._server:
            self._server.close()
            for w in list(self._writers):
                w.close()
            await self._server.wait_closed()

    async def _on_conn(self, reader, writer) -> None:
        self._writers.add(writer)
        lock = asyncio.Lock()
        inflight: set[asyncio.Task] = set()
        try:
            while (msg := await read_frame(reader)) is not None:
                # one task per request: a slow handler never blocks the connection
                t = asyncio.create_task(self._dispatch(msg, writer, lock))
                inflight.add(t)
                t.add_done_callback(inflight.discard)
        except (*_NET_ERRORS, RpcError) as e:
            log.debug("rpc connection closed: %r", e)
        finally:
            for t in list(inflight):
                t.cancel()
            self._writers.discard(writer)
            writer.close()

    async def _dispatch(self, msg: dict, writer, lock: asyncio.Lock) -> None:
        rid = msg.get("id")
        try:
            handler = self.handlers.get(msg.get("op"))
            if handler is None:
                raise RpcError(f"unknown op {msg.get('op')!r}")
            reply = {"id": rid, "ok": True, "res": await handler(msg.get("args") or {})}
        except Exception as e:
            log.warning("rpc op %r failed: %r", msg.get("op"), e)
            reply = {"id": rid, "ok": False, "err": f"{type(e).__name__}: {e}"}
        try:
            async with lock:
                writer.write(encode_frame(reply))
                await writer.drain()
        except (OSError, RpcError):
            pass  # caller is gone; nothing to do


# ---------------------------------------------------------------- client
class RpcClient:
    """One multiplexed connection to one peer, reconnecting on demand."""

    def __init__(self, host: str, port: int, timeout_s: float = 0.5):
        self.host, self.port, self.timeout = host, port, timeout_s
        self._writer: asyncio.StreamWriter | None = None
        self._read_task: asyncio.Task | None = None
        self._pending: dict[int, asyncio.Future] = {}
        self._ids = itertools.count(1)
        self._conn_lock = asyncio.Lock()
        self._write_lock = asyncio.Lock()
        self._last_rx = time.monotonic()
        self.connects = 0

    def _connected(self) -> bool:
        return self._writer is not None and not self._writer.is_closing()

    async def _connect(self) -> None:
        if self._connected():
            return
        async with self._conn_lock:
            if self._connected():
                return
            reader, writer = await asyncio.open_connection(self.host, self.port)
            self._writer = writer
            self._last_rx = time.monotonic()
            self.connects += 1
            self._read_task = asyncio.create_task(self._read_loop(reader, writer))

    async def _read_loop(self, reader, writer) -> None:
        err = "connection closed"
        try:
            while (msg := await read_frame(reader)) is not None:
                self._last_rx = time.monotonic()
                fut = self._pending.get(msg.get("id"))
                if fut is not None and not fut.done():
                    fut.set_result(msg)
        except (*_NET_ERRORS, RpcError) as e:
            err = repr(e)
        finally:
            if self._writer is writer:
                self._writer = None
            writer.close()
            for fut in list(self._pending.values()):  # fail everyone waiting on this socket
                if not fut.done():
                    fut.set_exception(RpcError(err))

    def _reset_if_wedged(self) -> None:
        """Nothing received for 3x the timeout while calls keep timing out: the socket is
        wedged (partition, frozen peer). Abort it so the next call dials fresh."""
        w = self._writer

        if w is not None and time.monotonic() - self._last_rx > 3 * self.timeout:
            w.transport.abort()

    async def call(
        self,
        op: str,
        args: dict | None = None,
        timeout: float | None = None,
    ) -> Any:
        rid = next(self._ids)
        fut = asyncio.get_running_loop().create_future()
        self._pending[rid] = fut

        where = f"{self.host}:{self.port}"

        log.debug("rpc -> %s id=%d op=%s", where, rid, op)

        try:
            async with asyncio.timeout(timeout or self.timeout):
                await self._connect()

                async with self._write_lock:
                    w = self._writer

                    if w is None or w.is_closing():
                        raise ConnectionError("not connected")

                    w.write(
                        encode_frame(
                            {
                                "id": rid,
                                "op": op,
                                "args": args or {},
                            }
                        )
                    )

                    await w.drain()

                msg = await fut

        except TimeoutError:
            self._reset_if_wedged()
            raise RpcError(f"{op} to {where} timed out") from None

        except OSError as e:
            raise RpcError(f"{op} to {where} failed: {e!r}") from None

        finally:
            self._pending.pop(rid, None)

        if not msg["ok"]:
            raise RpcError(msg["err"])

        return msg["res"]

    async def close(self) -> None:
        if self._read_task:
            self._read_task.cancel()
        if self._writer:
            self._writer.close()
        if self._read_task:
            await asyncio.gather(self._read_task, return_exceptions=True)


class Peers:
    """node_id -> RpcClient. Addresses come from the membership table."""

    def __init__(self, addrs: dict[str, tuple[str, int]] | None = None, timeout_s: float = 0.5):
        self._timeout = timeout_s
        self._clients: dict[str, RpcClient] = {}
        self._bg: set[asyncio.Task] = set()
        for nid, (h, p) in (addrs or {}).items():
            self.set_addr(nid, h, p)

    def set_addr(self, node_id: str, host: str, port: int) -> None:
        cur = self._clients.get(node_id)
        if cur is not None and (cur.host, cur.port) == (host, port):
            return
        if cur is not None:  # node came back on a new address
            t = asyncio.create_task(cur.close())
            self._bg.add(t)
            t.add_done_callback(self._bg.discard)
        self._clients[node_id] = RpcClient(host, port, self._timeout)

    async def call(self, node_id: str, op: str, args: dict | None = None, timeout=None):
        client = self._clients.get(node_id)
        if client is None:
            raise RpcError(f"unknown peer {node_id}")
        return await client.call(op, args, timeout)

    async def close(self) -> None:
        await asyncio.gather(*(c.close() for c in self._clients.values()))
        await asyncio.gather(*self._bg, return_exceptions=True)
