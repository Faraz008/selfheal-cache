import asyncio
import logging

from . import resp
from .config import NodeConfig
from .coordinator import ClusterError, Coordinator
from .rpc import RpcServer
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
        cfg.validate()

        self.cfg = cfg
        self.store = store if store is not None else Store(grace_ms=cfg.tombstone_grace_ms)

        self.coord = Coordinator(cfg, self.store)

        # Internal RPC server for communication between cluster nodes.
        self.rpc = RpcServer(
            cfg.host,
            cfg.rpc_port,
            self.coord.handlers(),
        )

        self._server: asyncio.Server | None = None
        self._sweeper: asyncio.Task | None = None
        self._writers: set[asyncio.StreamWriter] = set()

        # Command name: (handler, minimum arguments, maximum arguments)
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
            "COMMAND": (self.cmd_command, 0, None),
            # Debug commands (not part of Redis).
            "WHERE": (self.cmd_where, 1, 1),
            "LOCALGET": (self.cmd_localget, 1, 1),
        }

    # ---- lifecycle ----

    async def start(self) -> None:
        # Start internal RPC first so peers can reach this node.
        await self.rpc.start()

        # Start the client-facing RESP server.
        self._server = await asyncio.start_server(
            self._handle,
            self.cfg.host,
            self.cfg.port,
        )

        self._sweeper = asyncio.create_task(self._sweep_loop())

        log.info(
            "client port %d, RPC port %d, members=%s N=%d W=%d",
            self.port,
            self.rpc.port,
            sorted(self.coord.members),
            self.cfg.replicas,
            self.cfg.write_acks,
        )

    @property
    def port(self) -> int:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("Client server has not started")

        return self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        # Stop the background sweeper.
        if self._sweeper:
            self._sweeper.cancel()
            try:
                await self._sweeper
            except asyncio.CancelledError:
                pass
            self._sweeper = None

        # Stop accepting client connections.
        if self._server:
            self._server.close()

            for writer in list(self._writers):
                writer.close()

            await self._server.wait_closed()
            self._server = None

        # Stop internal RPC and close peer connections.
        await self.rpc.stop()
        await self.coord.close()

        log.info("stopped")

    async def _sweep_loop(self) -> None:
        interval = self.cfg.sweep_interval_ms / 1000

        while True:
            await asyncio.sleep(interval)

            removed = self.store.sweep()

            if removed:
                log.debug(
                    "sweeper collected %d records",
                    removed,
                )

    # ---- connection handling ----

    async def _handle(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
    ) -> None:
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

                writer.write(await self.execute(args))
                await writer.drain()

        except (asyncio.IncompleteReadError, ConnectionError):
            pass

        finally:
            self._writers.discard(writer)
            writer.close()

    async def execute(self, args: list[bytes]) -> bytes:
        name = args[0].upper().decode("utf-8", "replace")

        entry = self._table.get(name)

        if entry is None:
            return resp.error(f"ERR unknown command '{name}'")

        fn, lo, hi = entry
        n = len(args) - 1

        if n < lo or (hi is not None and n > hi):
            return resp.error(f"ERR wrong number of arguments for '{name.lower()}' command")

        try:
            return await fn(args[1:])

        except CommandError as e:
            return resp.error(str(e))

        except ClusterError as e:
            return resp.error(f"CLUSTERDOWN {e}")

    # ---- commands ----

    async def cmd_ping(self, a):
        return resp.bulk(a[0]) if a else resp.PONG

    async def cmd_command(self, a):
        return b"*0\r\n"

    async def cmd_get(self, a):
        rec = await self.coord.read(a[0])
        return resp.bulk(rec.value if rec else None)

    async def cmd_set(self, a):
        key, val = a[0], a[1]

        expire_at = None
        nx = False
        xx = False

        i = 2

        while i < len(a):
            opt = a[i].upper()

            if opt in (b"EX", b"PX"):
                if i + 1 >= len(a):
                    raise CommandError("ERR syntax error")

                n = parse_int(a[i + 1])

                if n <= 0:
                    raise CommandError("ERR invalid expire time in 'set' command")

                # Compute the absolute expiration timestamp once.
                expire_at = self.coord.now() + (n * 1000 if opt == b"EX" else n)

                i += 2

            elif opt == b"NX":
                nx = True
                i += 1

            elif opt == b"XX":
                xx = True
                i += 1

            else:
                raise CommandError("ERR syntax error")

        if nx and xx:
            raise CommandError("ERR syntax error")

        if nx or xx:
            # Best-effort only: NOT atomic in a leaderless cluster.
            exists = await self.coord.read(key) is not None

            if (nx and exists) or (xx and not exists):
                return resp.NIL

        await self.coord.set(key, val, expire_at)

        return resp.OK

    async def cmd_del(self, a):
        n = 0

        for k in a:
            if await self.coord.read(k) is not None:
                n += 1

            # Always write the tombstone.
            await self.coord.delete(k)

        return resp.integer(n)

    async def cmd_exists(self, a):
        n = 0

        for k in a:
            n += await self.coord.read(k) is not None

        return resp.integer(n)

    async def _expire(self, a, mult):
        n = parse_int(a[1])

        rec = await self.coord.read(a[0])

        if rec is None:
            return resp.integer(0)

        await self.coord.set(
            a[0],
            rec.value,
            self.coord.now() + n * mult,
        )

        return resp.integer(1)

    async def cmd_expire(self, a):
        return await self._expire(a, 1000)

    async def cmd_pexpire(self, a):
        return await self._expire(a, 1)

    async def _ttl_ms(self, key: bytes) -> int:
        rec = await self.coord.read(key)

        if rec is None:
            return -2

        if rec.expire_at is None:
            return -1

        return max(0, rec.expire_at - self.coord.now())

    async def cmd_pttl(self, a):
        return resp.integer(await self._ttl_ms(a[0]))

    async def cmd_ttl(self, a):
        ms = await self._ttl_ms(a[0])

        return resp.integer(ms if ms < 0 else (ms + 500) // 1000)

    async def cmd_persist(self, a):
        rec = await self.coord.read(a[0])

        if rec is None or rec.expire_at is None:
            return resp.integer(0)

        await self.coord.set(a[0], rec.value, None)

        return resp.integer(1)

    async def cmd_dbsize(self, a):
        # Counts records stored on THIS node only.
        return resp.integer(len(self.store))

    async def cmd_where(self, a):
        return resp.array([resp.bulk(n.encode()) for n in self.coord.preference_list(a[0])])

    async def cmd_localget(self, a):
        # Reads THIS node's local store, bypassing the coordinator.
        rec = self.store.get_live(a[0])

        return resp.bulk(rec.value if rec else None)
