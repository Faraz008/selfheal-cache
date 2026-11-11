import random
import time
from collections.abc import Callable
from dataclasses import dataclass

from .evict import make_policy, record_size

Version = tuple[int, str]  # (hlc_timestamp, node_id); higher tuple wins


def now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass(frozen=True, slots=True)
class Record:
    value: bytes | None  # None = tombstone
    version: Version
    expire_at: int | None = None  # ABSOLUTE wall-clock ms; for a tombstone, the delete time

    @property
    def is_tombstone(self) -> bool:
        return self.value is None

    def is_live(self, now: int) -> bool:
        return self.value is not None and (self.expire_at is None or self.expire_at > now)


class KeyIndex:
    """Set with O(1) add/discard and O(k) uniform random sampling (swap-remove)."""

    def __init__(self):
        self._keys: list[bytes] = []
        self._pos: dict[bytes, int] = {}

    def __len__(self) -> int:
        return len(self._keys)

    def __contains__(self, k: bytes) -> bool:
        return k in self._pos

    def add(self, k: bytes) -> None:
        if k not in self._pos:
            self._pos[k] = len(self._keys)
            self._keys.append(k)

    def discard(self, k: bytes) -> None:
        i = self._pos.pop(k, None)
        if i is None:
            return
        last = self._keys.pop()
        if i < len(self._keys):
            self._keys[i] = last
            self._pos[last] = i

    def sample(self, n: int) -> list[bytes]:
        return random.sample(self._keys, min(n, len(self._keys)))


class Store:
    def __init__(
        self,
        clock: Callable[[], int] = now_ms,
        grace_ms: int = 600_000,
        max_bytes: int = 0,
        policy: str = "lru",
    ):
        self.now = clock  # injectable so tests control time
        self.grace_ms = grace_ms
        self.max_bytes = max_bytes
        self._data: dict[bytes, Record] = {}
        self._expiring = KeyIndex()  # keys whose record has an expire_at
        # The policy tracks EVALUABLE keys only: records that still carry a value.
        self._policy = make_policy(policy) if max_bytes > 0 else None
        self.mem = 0
        self.evictions = 0
        self.evict_blocked = 0  # times we were over the cap with nothing evictable
        self.shrunk = 0  # expired payloads reclaimed

    # ---- reads ----
    def __len__(self) -> int:
        """Live keys only (O(n); DBSIZE is a debug command)."""
        now = self.now()
        return sum(1 for r in self._data.values() if r.is_live(now))

    def raw_len(self) -> int:
        return len(self._data)

    def get_record(self, key: bytes, touch: bool = False) -> Record | None:
        """Raw record (tombstones and expired included). touch=True counts the access
        for eviction: only client-serving reads should pass it."""
        rec = self._data.get(key)
        if touch and rec is not None and self._policy is not None and rec.value is not None:
            self._policy.touch(key)
        return rec

    def get_live(self, key: bytes) -> Record | None:
        rec = self._data.get(key)
        return rec if rec is not None and rec.is_live(self.now()) else None

    # ---- internals ----
    def _collectable(self, rec: Record, now: int) -> bool:
        return rec.expire_at is not None and rec.expire_at + self.grace_ms <= now

    def _remove(self, key: bytes) -> None:
        rec = self._data.pop(key, None)
        if rec is not None:
            self.mem -= record_size(key, rec)
        self._expiring.discard(key)
        if self._policy is not None:
            self._policy.remove(key)

    def _put(self, key: bytes, rec: Record, cur: Record | None) -> None:
        if cur is not None:
            self.mem -= record_size(key, cur)
        self._data[key] = rec
        self.mem += record_size(key, rec)
        if rec.expire_at is None:
            self._expiring.discard(key)
        else:
            self._expiring.add(key)
        if self._policy is not None:
            if rec.value is None:
                self._policy.remove(key)  # tombstones are pinned, never evictable
            else:
                self._policy.add(key)

    def _shrink(self, key: bytes, rec: Record) -> None:
        """Expired value -> bare tombstone: same version, same deadline, no payload.
        It still blocks resurrection but the bytes are reclaimed."""
        small = Record(None, rec.version, rec.expire_at)
        self.mem += record_size(key, small) - record_size(key, rec)
        self._data[key] = small
        if self._policy is not None:
            self._policy.remove(key)
        self.shrunk += 1

    def _enforce(self, protect: bytes | None = None) -> None:
        if self._policy is None or self.mem <= self.max_bytes:
            return
        now = self.now()
        while self.mem > self.max_bytes:
            k = self._policy.victim(protect)
            if k is None:  # only pinned records (or the key just written) remain
                self.evict_blocked += 1
                return
            rec = self._data.get(k)
            if rec is None:  # defensive: policy and data must never disagree
                self._policy.remove(k)
                continue
            if not rec.is_live(now):
                self._shrink(k, rec)  # reclaim the payload, keep the version
            else:
                self._remove(k)  # plain delete: NO tombstone, this is a cache miss
                self.evictions += 1

    # ---- writes ----
    def apply(self, key: bytes, rec: Record) -> bool:
        """Last-write-wins merge. Idempotent. Returns True if state changed."""
        cur = self._data.get(key)
        if cur is not None and cur.version >= rec.version:
            return False
        if self._collectable(rec, self.now()):
            if cur is not None:
                self._remove(key)
            return False
        self._put(key, rec, cur)
        self._enforce(protect=key)  # never evict the record we were just handed
        return True

    def sweep(self, sample: int = 20, max_rounds: int = 16) -> int:
        """Active maintenance over records with a deadline: collect those past
        expire_at + grace, and shrink expired payloads that are still inside the grace."""
        removed = 0
        for _ in range(max_rounds):
            if not len(self._expiring):
                break
            keys = self._expiring.sample(sample)
            now = self.now()
            dead, shrunk = [], 0
            for k in keys:
                rec = self._data[k]
                if self._collectable(rec, now):
                    dead.append(k)
                elif rec.value is not None and not rec.is_live(now):
                    self._shrink(k, rec)
                    shrunk += 1
            for k in dead:
                self._remove(k)
            removed += len(dead)
            if (len(dead) + shrunk) * 4 <= len(keys):
                break
        return removed

    def age(self) -> None:
        if self._policy is not None:
            self._policy.age()

    # ---- rebalancing helpers (unchanged from Phase 6) ----
    def keys_snapshot(self) -> list[bytes]:
        return list(self._data)

    def transferable(self, key: bytes) -> Record | None:
        rec = self._data.get(key)
        now = self.now()
        if rec is None or self._collectable(rec, now):
            return None
        if rec.value is not None and not rec.is_live(now):
            return Record(None, rec.version, rec.expire_at)
        return rec

    def drop_if_version(self, key: bytes, version: Version) -> bool:
        cur = self._data.get(key)
        if cur is None or cur.version != version:
            return False
        self._remove(key)
        return True
