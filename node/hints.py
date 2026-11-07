import itertools
import logging

from .store import Record, now_ms

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
