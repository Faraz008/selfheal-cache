import asyncio
import logging

from . import resp
from .config import NodeConfig
from .coordinator import ClusterError, Coordinator
from .detector import FailureDetector
from .gossip import Gossiper
from .hints import HintReplayer, HintStore
from .membership import Membership, State
from .rebalance import RebalanceError, Rebalancer
from .rpc import Peers, RpcServer
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

        # ---- membership and cluster communication ----

        self.membership = Membership.from_config(
            cfg,
            self.store.now,
        )

        self.peers = Peers(
            timeout_s=cfg.rpc_timeout_ms / 1000,
        )

        self.hints = HintStore(
            cfg.max_hints,
            cfg.hint_ttl_ms,
            self.store.now,
        )

        self.coord = Coordinator(
            cfg,
            self.store,
            self.membership,
            self.peers,
            self.hints,
        )

        self.gossip = Gossiper(
            cfg,
            self.membership,
            self.peers,
        )

        self.replayer = HintReplayer(
            cfg,
            self.membership,
            self.peers,
            self.hints,
        )

        # Rebalancer initialization
        self.rebalancer = Rebalancer(
            cfg,
            self.store,
            self.membership,
            self.coord,
            self.peers,
            self.gossip,
        )

        self.gossip.on_bootstrapped = self.rebalancer.join

        self.detector = FailureDetector(
            cfg,
            self.membership,
            self.peers,
            self.gossip,
        )

        self.rpc = RpcServer(
            cfg.host,
            cfg.rpc_port,
            {
                **self.coord.handlers(),
                **self.gossip.handlers(),
                **self.detector.handlers(),
                **self.rebalancer.handlers(),
            },
        )

        # ---- client-facing server lifecycle ----

        self._server: asyncio.Server | None = None
        self._sweeper: asyncio.Task | None = None
        self._writers: set[asyncio.StreamWriter] = set()

        # Signals that this node has completed graceful leave.
        self.left = asyncio.Event()

        # Command name: (handler, minimum arguments, maximum arguments)
        self._table = {
            "PING": (self.cmd_ping, 0, 1),
            "SET": (self.cmd_set, 2, None),
            "GET": (self.cmd_get, 1, 1),
            "DEL": (self.cmd_del, 1, None),
            "EXISTS": (self.cmd_exists, 1, None),
            "EXPIRE": (self.cmd_expire, 2, 2),
            "PEXPIRE": (self.cmd_pexpire, 2, 2),
            "EXPIREAT": (self.cmd_expireat, 2, 2),
            "PEXPIREAT": (self.cmd_pexpireat, 2, 2),
            "TTL": (self.cmd_ttl, 1, 1),
            "PTTL": (self.cmd_pttl, 1, 1),
            "PERSIST": (self.cmd_persist, 1, 1),
            "DBSIZE": (self.cmd_dbsize, 0, 0),
            "COMMAND": (self.cmd_command, 0, None),
            "INFO": (self.cmd_info, 0, 0),
            "CLUSTER": (self.cmd_cluster, 1, 2),
            # Debug commands (not part of Redis).
            "WHERE": (self.cmd_where, 1, 1),
            "LOCALGET": (self.cmd_localget, 1, 1),
        }

    # ---- lifecycle ----

    async def start(self) -> None:
        # Start internal RPC first so peers can reach this node.
        await self.rpc.start()

        # Start membership gossip after RPC is available.
        await self.gossip.start()
        await self.detector.start()
        await self.replayer.start()

        # Start rebalancing after hint replay starts.
        await self.rebalancer.start()

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
            self.membership.node_ids(),
            self.cfg.replicas,
            self.cfg.write_acks,
        )

    @property
    def port(self) -> int:
        if self._server is None or not self._server.sockets:
            raise RuntimeError("Client server has not started")

        return self._server.sockets[0].getsockname()[1]

    async def stop(self) -> None:
        # Stop rebalancer first.
        await self.rebalancer.stop()

        # Stop gossip before shutting down internal RPC.
        await self.replayer.stop()
        await self.detector.stop()
        await self.gossip.stop()

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

        # Stop internal RPC.
        await self.rpc.stop()

        # Close coordinator and peer connections.
        await self.coord.close()
        await self.peers.close()

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

    async def cmd_info(self, a):
        s = self.coord.stats

        lines = [
            f"node_id:{self.cfg.node_id}",
            f"live_keys:{len(self.store)}",
            f"raw_records:{self.store.raw_len()}",
            f"hints_held:{self.hints.count}",
            f"hints_rejected:{self.hints.rejected}",
            f"hints_written:{s['hints_written']}",
            f"read_repairs:{s['read_repairs']}",
        ]

        r = self.rebalancer.stats

        lines += [
            f"state:{self.membership.self_entry.state.value}",
            f"ring_nodes:{len(self.coord.ring)}",
            f"keys_received:{r['keys_received']}",
            f"keys_sent:{r['keys_sent']}",
            f"keys_pushed:{r['keys_pushed']}",
            f"keys_cleaned:{r['keys_cleaned']}",
        ]

        return resp.bulk(("\n".join(lines) + "\n").encode())

    async def cmd_cluster(self, a):
        sub = a[0].upper()

        if sub == b"NODES" and len(a) == 1:
            lines = [
                f"{e.node_id} {e.host}:{e.port} {e.state.value} {e.incarnation}"
                + (" myself" if e.node_id == self.cfg.node_id else "")
                for e in sorted(
                    self.membership.entries(),
                    key=lambda e: e.node_id,
                )
            ]

            return resp.bulk(("\n".join(lines) + "\n").encode())

        if sub == b"LEAVE" and len(a) == 1:
            # Blocks until the handoff is complete.
            try:
                await self.rebalancer.leave()
            except RebalanceError as e:
                raise CommandError(f"ERR {e}") from None

            self.left.set()
            return resp.OK

        if sub == b"FORGET" and len(a) == 2:
            nid = a[1].decode("utf-8", "replace")
            st = self.membership.state_of(nid)

            if st is None:
                raise CommandError("ERR unknown node")

            if nid == self.cfg.node_id or st not in (State.DEAD, State.LEAVING):
                raise CommandError("ERR node is not dead (use CLUSTER LEAVE on a live node)")

            self.membership.forget(nid)
            return resp.OK

        if sub == b"SYNC" and len(a) == 1:
            # Re-pull every range this node owns.
            try:
                return resp.integer(await self.rebalancer.catch_up())
            except RebalanceError as e:
                raise CommandError(f"ERR {e}") from None

        raise CommandError("ERR unknown CLUSTER subcommand")

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

            if opt in (b"EX", b"PX", b"EXAT", b"PXAT"):
                if i + 1 >= len(a):
                    raise CommandError("ERR syntax error")
                n = parse_int(a[i + 1])
                if n <= 0:
                    raise CommandError("ERR invalid expire time in 'set' command")
                if opt == b"EX":
                    expire_at = self.coord.now() + n * 1000  # relative -> absolute, ONCE, here
                elif opt == b"PX":
                    expire_at = self.coord.now() + n
                else:
                    expire_at = n * 1000 if opt == b"EXAT" else n  # client gave the deadline
                i += 2

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

    async def _set_expiry(self, key: bytes, ts: int) -> bytes:
        rec = await self.coord.read(key)
        if rec is None:
            return resp.integer(0)
        if ts <= self.coord.now():
            await self.coord.delete(key)  # Redis semantics: a deadline in the past deletes
        else:
            await self.coord.set(key, rec.value, ts)
        return resp.integer(1)

    async def _expire(self, a, mult):
        return await self._set_expiry(a[0], self.coord.now() + parse_int(a[1]) * mult)

    async def _expire_at(self, a, mult):
        return await self._set_expiry(a[0], parse_int(a[1]) * mult)

    async def cmd_expire(self, a):
        return await self._expire(a, 1000)

    async def cmd_pexpire(self, a):
        return await self._expire(a, 1)

    async def cmd_expireat(self, a):
        return await self._expire_at(a, 1000)

    async def cmd_pexpireat(self, a):
        return await self._expire_at(a, 1)

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
