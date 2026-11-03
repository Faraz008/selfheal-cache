import asyncio
import logging

from . import resp
from .config import NodeConfig
from .store import Store

log = logging.getLogger("node")


class CommandError(Exception):
    pass


def parse_int(raw: bytes) -> int:
    try:
        return int(raw)
    except ValueError:
        raise CommandError("ERR value is not an integer or out of range") from None


class Server:
    def __init__(self, cfg: NodeConfig, store: Store | None = None):
        self.cfg = cfg
        self.store = store or Store()
        self._server: asyncio.Server | None = None
        self._sweeper: asyncio.Task | None = None
        self._writers: set[asyncio.StreamWriter] = set()
        # name: (handler, min_args, max_args)
        self._table = {
            "PING": (self.cmd_ping, 0, 1),
            "SET": (self.cmd_set, 2, None),
            "GET": (self.cmd_get, 1, 1),
            "DEL": (self.cmd_del, 1, None),
            "EXISTS": (self.cmd_exists, 1, None),
            "EXPIRE": (self.cmd_expire, 2, 2),
            "PEXPIRE": (self.cmd_pexpire, 2, 2),
            "TTL": (self.cmd_ttl, 1, 1),
            "PTTL": (self.cmd_pttl, 1, 1),
            "PERSIST": (self.cmd_persist, 1, 1),
            "DBSIZE": (self.cmd_dbsize, 0, 0),
            "COMMAND": (self.cmd_command, 0, None),  # redis-cli sends this on connect
        }

    # ---- lifecycle ----
    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.cfg.host, self.cfg.port)
        self._sweeper = asyncio.create_task(self._sweep_loop())
        log.info("listening on %s:%d", self.cfg.host, self.port)

    @property
    def port(self) -> int:
        return self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        if self._sweeper:
            self._sweeper.cancel()
        if self._server:
            self._server.close()
            for w in list(self._writers):
                w.close()
            await self._server.wait_closed()
        log.info("stopped")

    async def _sweep_loop(self) -> None:
        interval = self.cfg.sweep_interval_ms / 1000
        while True:
            await asyncio.sleep(interval)
            removed = self.store.sweep()
            if removed:
                log.debug("sweeper removed %d keys", removed)

    # ---- connection handling ----
    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._writers.add(writer)
        try:
            while True:
                try:
                    args = await resp.read_command(reader)
                except resp.ProtocolError as e:
                    writer.write(resp.error(f"ERR Protocol error: {e}"))
                    await writer.drain()
                    break
                if args is None:
                    break
                if not args:
                    continue
                writer.write(self.execute(args))
                await writer.drain()
        except (asyncio.IncompleteReadError, ConnectionError):
            pass
        finally:
            self._writers.discard(writer)
            writer.close()

    def execute(self, args: list[bytes]) -> bytes:
        name = args[0].upper().decode("utf-8", "replace")
        entry = self._table.get(name)
        if entry is None:
            return resp.error(f"ERR unknown command '{name}'")
        fn, lo, hi = entry
        n = len(args) - 1
        if n < lo or (hi is not None and n > hi):
            return resp.error(f"ERR wrong number of arguments for '{name.lower()}' command")
        try:
            return fn(args[1:])
        except CommandError as e:
            return resp.error(str(e))

    # ---- commands ----
    def cmd_ping(self, a):
        return resp.bulk(a[0]) if a else resp.PONG

    def cmd_command(self, a):
        return b"*0\r\n"

    def cmd_get(self, a):
        return resp.bulk(self.store.get(a[0]))

    def cmd_set(self, a):
        key, val = a[0], a[1]
        expire_at, nx, xx = None, False, False
        i = 2
        while i < len(a):
            opt = a[i].upper()
            if opt in (b"EX", b"PX"):
                if i + 1 >= len(a):
                    raise CommandError("ERR syntax error")
                n = parse_int(a[i + 1])
                if n <= 0:
                    raise CommandError("ERR invalid expire time in 'set' command")
                expire_at = self.store.now() + (n * 1000 if opt == b"EX" else n)
                i += 2
            elif opt == b"NX":
                nx, i = True, i + 1
            elif opt == b"XX":
                xx, i = True, i + 1
            else:
                raise CommandError("ERR syntax error")
        if nx and xx:
            raise CommandError("ERR syntax error")
        exists = self.store.get(key) is not None
        if (nx and exists) or (xx and not exists):
            return resp.NIL
        self.store.set(key, val, expire_at)
        return resp.OK

    def cmd_del(self, a):
        return resp.integer(sum(self.store.delete(k) for k in a))

    def cmd_exists(self, a):
        return resp.integer(sum(self.store.get(k) is not None for k in a))

    def _expire(self, a, mult):
        n = parse_int(a[1])
        return resp.integer(int(self.store.set_expire_at(a[0], self.store.now() + n * mult)))

    def cmd_expire(self, a):
        return self._expire(a, 1000)

    def cmd_pexpire(self, a):
        return self._expire(a, 1)

    def cmd_pttl(self, a):
        return resp.integer(self.store.ttl_ms(a[0]))

    def cmd_ttl(self, a):
        ms = self.store.ttl_ms(a[0])
        return resp.integer(ms if ms < 0 else (ms + 500) // 1000)

    def cmd_persist(self, a):
        return resp.integer(int(self.store.persist(a[0])))

    def cmd_dbsize(self, a):
        return resp.integer(len(self.store))
