import asyncio
import itertools
import logging

from .config import NodeConfig
from .membership import Membership, State
from .rpc import Peers, RpcError
from .store import Record, now_ms
from .wire import b64e, rec_to_wire

log = logging.getLogger("hints")


class HintStore:
    """Writes held on behalf of a down node. Newest version per (owner, key) only,
    which is all LWW needs. In-memory: hints die with this process."""

    def __init__(self, max_hints: int, ttl_ms: int, clock=now_ms):
        self.max, self.ttl, self.now = max_hints, ttl_ms, clock
        self._h: dict[str, dict[bytes, tuple[int, Record]]] = {}  # owner -> key -> (stored_at, rec)
        self.count = 0
        self.rejected = 0

    def add(self, owner: str, key: bytes, rec: Record) -> bool:
        bucket = self._h.setdefault(owner, {})
        cur = bucket.get(key)
        if cur is not None:
            if cur[1].version >= rec.version:
                return True  # already holding something at least as new
            bucket[key] = (self.now(), rec)
            return True
        if self.count >= self.max:
            self.rejected += 1
            if not bucket:
                del self._h[owner]
            return False  # backpressure: caller treats this slot as failed
        bucket[key] = (self.now(), rec)
        self.count += 1
        return True

    def owners(self) -> list[str]:
        return list(self._h)

    def pending(self, owner: str, limit: int) -> list[tuple[bytes, Record]]:
        bucket = self._h.get(owner, {})
        return list(itertools.islice(((k, r) for k, (_, r) in bucket.items()), limit))

    def done(self, owner: str, key: bytes, rec: Record) -> None:
        """Delivered. Forget it unless a NEWER hint for the same key arrived meanwhile."""
        bucket = self._h.get(owner)
        cur = bucket.get(key) if bucket else None
        if cur is not None and cur[1].version == rec.version:
            del bucket[key]
            self.count -= 1
            if not bucket:
                del self._h[owner]

    def expire(self) -> int:
        cutoff = self.now() - self.ttl
        n = 0
        for owner, bucket in list(self._h.items()):
            for k, (t, _) in list(bucket.items()):
                if t <= cutoff:
                    del bucket[k]
                    n += 1
            if not bucket:
                del self._h[owner]
        self.count -= n
        return n


class HintReplayer:
    """Delivers held hints to their owner once the owner is ALIVE again."""

    def __init__(self, cfg: NodeConfig, membership: Membership, peers: Peers, hints: HintStore):
        self.cfg, self.m, self.peers, self.hints = cfg, membership, peers, hints
        self.wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        membership.subscribe(self._on_change)

    def _on_change(self, changed) -> None:
        if any(e.state is State.ALIVE for e in changed):
            self.wake.set()  # an owner just came back: replay NOW, don't wait for the timer

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)
            self._task = None

    async def _loop(self) -> None:
        interval = self.cfg.hint_replay_interval_ms / 1000
        while True:
            try:
                await asyncio.wait_for(self.wake.wait(), interval)
            except TimeoutError:
                pass
            self.wake.clear()
            try:
                dropped = self.hints.expire()
                if dropped:
                    log.warning("dropped %d expired hints", dropped)
                if await self.replay_once():
                    self.wake.set()  # more may remain; go again, but pace ourselves
                    await asyncio.sleep(0.05)
            except Exception:
                log.exception("hint replay failed")

    async def replay_once(self) -> int:
        sent = 0
        for owner in self.hints.owners():
            e = self.m.get(owner)
            if e is None or e.state is not State.ALIVE:
                continue  # wrong time; the hints wait
            batch = self.hints.pending(owner, self.cfg.hint_batch)
            if not batch:
                continue
            items = [{"k": b64e(k), "r": rec_to_wire(r)} for k, r in batch]
            try:
                await self.peers.call(owner, "put_many", {"items": items})
            except RpcError as err:
                log.debug("replay to %s failed: %s", owner, err)
                continue  # keep the hints, try later
            for k, r in batch:
                self.hints.done(owner, k, r)
            sent += len(batch)
            log.info("replayed %d hints to %s", len(batch), owner)
        return sent
