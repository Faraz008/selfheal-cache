import asyncio
import itertools
import logging
import time
from contextlib import asynccontextmanager

log = logging.getLogger("client")


class RespError(Exception):
    """The server answered with an error reply."""


class ClusterUnavailable(Exception):
    """Every attempt (initial plus retries) failed."""


def encode(parts) -> bytes:
    out = [b"*%d\r\n" % len(parts)]
    for p in parts:
        if isinstance(p, str):
            p = p.encode()
        elif isinstance(p, int):
            p = str(p).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(p), p))
    return b"".join(out)


async def read_reply(r: asyncio.StreamReader):
    line = await r.readline()
    if not line:
        raise ConnectionError("connection closed")
    t, rest = line[:1], line[1:-2]
    if t == b"+":
        return rest.decode()
    if t == b"-":
        return RespError(rest.decode())  # returned, not raised: keeps the connection healthy
    if t == b":":
        return int(rest)
    if t == b"$":
        n = int(rest)
        return None if n < 0 else (await r.readexactly(n + 2))[:-2]
    if t == b"*":
        n = int(rest)
        return None if n < 0 else [await read_reply(r) for _ in range(n)]
    raise ConnectionError(f"bad reply {line!r}")


class _Pool:
    def __init__(self, host: str, port: int, size: int, connect_timeout: float):
        self.host, self.port, self.connect_timeout = host, port, connect_timeout
        self._sem = asyncio.Semaphore(size)
        self._idle: list[tuple[asyncio.StreamReader, asyncio.StreamWriter]] = []

    @asynccontextmanager
    async def conn(self):
        async with self._sem:
            rw = None
            while self._idle:
                cand = self._idle.pop()
                if not cand[1].is_closing():
                    rw = cand
                    break
            if rw is None:
                rw = await asyncio.wait_for(
                    asyncio.open_connection(self.host, self.port), self.connect_timeout
                )
            try:
                yield rw
            except BaseException:
                rw[1].close()  # unknown protocol state (timeout, EOF): never reuse
                raise
            else:
                self._idle.append(rw)

    def close(self) -> None:
        for _, w in self._idle:
            w.close()
        self._idle.clear()


class ClusterClient:
    def __init__(
        self,
        seeds: list[tuple[str, int]],
        retries: int = 1,
        request_timeout: float = 2.0,
        refresh_interval: float = 5.0,
        penalty: float = 3.0,
        pool_size: int = 8,
    ):
        self._seeds = [f"{h}:{p}" for h, p in seeds]
        self._nodes: dict[str, str] = {a: "alive" for a in self._seeds}  # "host:port" -> state
        self._pools: dict[str, _Pool] = {}
        self._down_until: dict[str, float] = {}
        self._rr = itertools.count()
        self.retries, self.request_timeout = retries, request_timeout
        self.refresh_interval, self.penalty, self.pool_size = refresh_interval, penalty, pool_size
        self.stats = {"requests": 0, "retries": 0, "failures": 0}
        self._task: asyncio.Task | None = None

    # ---- lifecycle ----
    async def start(self) -> None:
        await self.refresh()
        self._task = asyncio.create_task(self._loop())

    async def close(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
        for p in self._pools.values():
            p.close()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, *exc):
        await self.close()

    async def _loop(self) -> None:
        while True:
            await asyncio.sleep(self.refresh_interval)
            try:
                await self.refresh()
            except Exception:
                log.exception("refresh failed")

    # ---- topology ----
    def nodes(self) -> list[str]:
        return list(self._nodes)

    async def refresh(self) -> bool:
        """Learn the live node list from any reachable node (known ones first, then seeds)."""
        for addr in [*self._nodes, *(s for s in self._seeds if s not in self._nodes)]:
            try:
                reply = await self._send(addr, ("CLUSTER", "NODES"))
            except (OSError, EOFError):
                continue
            if isinstance(reply, RespError) or not reply:
                continue
            found: dict[str, str] = {}
            for line in reply.decode().splitlines():
                f = line.split()  # id host:port state incarnation [myself]
                if len(f) >= 3 and f[2] in ("alive", "suspect"):
                    found[f[1]] = f[2]
            if found:
                self._nodes = found
                return True
        return False

    def _pick(self, exclude: list[str]) -> str | None:
        now = time.monotonic()
        cands = [a for a in self._nodes if a not in exclude]
        healthy = [a for a in cands if self._down_until.get(a, 0) <= now]
        pool = healthy or cands or list(self._nodes)  # last resort: reuse a node
        return pool[next(self._rr) % len(pool)] if pool else None

    def _pool(self, addr: str) -> _Pool:
        p = self._pools.get(addr)
        if p is None:
            host, port = addr.rsplit(":", 1)
            p = self._pools[addr] = _Pool(host, int(port), self.pool_size, 1.0)
        return p

    async def _send(self, addr: str, parts):
        async with asyncio.timeout(self.request_timeout):
            async with self._pool(addr).conn() as (r, w):
                w.write(encode(parts))
                await w.drain()
                return await read_reply(r)

    # ---- the retry engine ----
    async def execute(self, *parts):
        self.stats["requests"] += 1
        tried: list[str] = []
        last: Exception | None = None
        for attempt in range(self.retries + 1):
            addr = self._pick(tried)
            if addr is None:
                break
            tried.append(addr)
            if attempt:
                self.stats["retries"] += 1
            try:
                reply = await self._send(addr, parts)
            except (OSError, EOFError) as e:  # refused, reset, EOF, timeout
                last = e
                self._down_until[addr] = time.monotonic() + self.penalty
                continue
            if isinstance(reply, RespError):
                if str(reply).startswith("CLUSTERDOWN"):  # this coordinator can't make quorum
                    last = reply
                    continue
                raise reply  # a real command error: retrying won't help
            return reply
        self.stats["failures"] += 1
        raise ClusterUnavailable(f"{len(tried)} attempt(s) failed: {last!r}") from last

    # ---- typed API ----
    async def set(self, key, value, ex: int | None = None, px: int | None = None) -> bool:
        parts = ["SET", key, value]
        if ex is not None:
            parts += ["EX", ex]
        if px is not None:
            parts += ["PX", px]
        return await self.execute(*parts) == "OK"

    async def get(self, key) -> bytes | None:
        return await self.execute("GET", key)

    async def delete(self, *keys) -> int:
        return await self.execute("DEL", *keys)

    async def exists(self, *keys) -> int:
        return await self.execute("EXISTS", *keys)

    async def ttl(self, key) -> int:
        return await self.execute("TTL", key)

    async def expire(self, key, seconds: int) -> bool:
        return await self.execute("EXPIRE", key, seconds) == 1
