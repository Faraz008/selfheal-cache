import random
import time
from collections.abc import Callable
from dataclasses import dataclass

Version = tuple[int, str]  # (timestamp_ms, node_id); higher tuple wins


def now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(frozen=True, slots=True)
class Record:
    value: bytes | None  # None = tombstone
    version: Version
    expire_at: int | None = None  # ABSOLUTE ms; for a tombstone, the delete time

    @property
    def is_tombstone(self) -> bool:
        return self.value is None

    def is_live(self, now: int) -> bool:
        return self.value is not None and (self.expire_at is None or self.expire_at > now)


class VersionClock:
    """Strictly increasing versions for writes coordinated by this node,
    even for several writes in the same millisecond."""

    def __init__(self, node_id: str, clock: Callable[[], int] = now_ms):
        self.node_id = node_id
        self._clock = clock
        self._last = 0

    def next(self) -> Version:
        self._last = max(self._clock(), self._last + 1)
        return (self._last, self.node_id)


class Store:
    def __init__(self, clock: Callable[[], int] = now_ms, grace_ms: int = 600_000):
        self.now = clock  # injectable so tests control time
        self.grace_ms = grace_ms
        self._data: dict[bytes, Record] = {}
        self._expiring: set[bytes] = set()  # keys whose record has an expire_at

    def __len__(self) -> int:
        """Live keys only (O(n); fine for now, DBSIZE is a debug command)."""
        now = self.now()
        return sum(1 for r in self._data.values() if r.is_live(now))

    def raw_len(self) -> int:
        """Everything held, including tombstones and expired-but-not-collected."""
        return len(self._data)

    def get_record(self, key: bytes) -> Record | None:
        """Raw record, possibly a tombstone or expired. Replication needs these."""
        return self._data.get(key)

    def get_live(self, key: bytes) -> Record | None:
        rec = self._data.get(key)
        return rec if rec is not None and rec.is_live(self.now()) else None

    def _collectable(self, rec: Record, now: int) -> bool:
        return rec.expire_at is not None and rec.expire_at + self.grace_ms <= now

    def _remove(self, key: bytes) -> None:
        self._data.pop(key, None)
        self._expiring.discard(key)

    def apply(self, key: bytes, rec: Record) -> bool:
        """Last-write-wins merge. Idempotent. Returns True if state changed."""
        cur = self._data.get(key)
        if cur is not None and cur.version >= rec.version:
            return False
        if self._collectable(rec, self.now()):
            # Newer than what we hold but already past its grace: the key is
            # dead, so drop the older value rather than store garbage.
            if cur is not None:
                self._remove(key)
            return False
        self._data[key] = rec
        if rec.expire_at is None:
            self._expiring.discard(key)
        else:
            self._expiring.add(key)
        return True

    def sweep(self, sample: int = 20, max_rounds: int = 16) -> int:
        """Redis-style active GC over records that are past expire_at + grace."""
        removed = 0
        for _ in range(max_rounds):
            if not self._expiring:
                break
            keys = random.sample(list(self._expiring), min(sample, len(self._expiring)))
            now = self.now()
            dead = [k for k in keys if self._collectable(self._data[k], now)]
            for k in dead:
                self._remove(k)
            removed += len(dead)
            if len(dead) * 4 <= len(keys):
                break
        return removed

    def keys_snapshot(self) -> list[bytes]:
        """A copy, so callers can iterate across awaits while writes continue."""
        return list(self._data)

    def transferable(self, key: bytes) -> Record | None:
        """The record to ship during rebalancing, or None to skip it.
        - already collectable (past expire_at + grace): skip
        - expired value: ship as a bare tombstone (same version, no payload); it still
          blocks resurrection but the expired bytes never cross the network."""
        rec = self._data.get(key)
        now = self.now()
        if rec is None or self._collectable(rec, now):
            return None
        if rec.value is not None and not rec.is_live(now):
            return Record(None, rec.version, rec.expire_at)
        return rec

    def drop_if_version(self, key: bytes, version: Version) -> bool:
        """Delete only if the record is still exactly this version. A newer write that
        arrived meanwhile must survive."""
        cur = self._data.get(key)
        if cur is None or cur.version != version:
            return False
        self._remove(key)
        return True
