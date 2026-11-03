import random
import time
from collections.abc import Callable


def now_ms() -> int:
    return time.time_ns() // 1_000_000


class Store:
    def __init__(self, clock: Callable[[], int] = now_ms):
        self.now = clock  # injectable so tests control time
        self._data: dict[bytes, bytes] = {}
        self._expires: dict[bytes, int] = {}  # key -> ABSOLUTE ms timestamp

    def __len__(self) -> int:
        return len(self._data)  # may include expired-but-unswept keys

    def _drop_if_expired(self, key: bytes) -> bool:
        exp = self._expires.get(key)
        if exp is not None and exp <= self.now():
            self._data.pop(key, None)
            del self._expires[key]
            return True
        return False

    def get(self, key: bytes) -> bytes | None:
        self._drop_if_expired(key)
        return self._data.get(key)

    def set(self, key: bytes, value: bytes, expire_at_ms: int | None = None) -> None:
        self._data[key] = value
        if expire_at_ms is None:
            self._expires.pop(key, None)  # plain SET clears any TTL
        else:
            self._expires[key] = expire_at_ms

    def delete(self, key: bytes) -> bool:
        self._drop_if_expired(key)
        existed = key in self._data
        self._data.pop(key, None)
        self._expires.pop(key, None)
        return existed

    def set_expire_at(self, key: bytes, ts_ms: int) -> bool:
        if self.get(key) is None:
            return False
        self._expires[key] = ts_ms
        return True

    def persist(self, key: bytes) -> bool:
        if self.get(key) is None:
            return False
        return self._expires.pop(key, None) is not None

    def ttl_ms(self, key: bytes) -> int:
        """-2 = missing, -1 = no expiry, else remaining ms."""
        if self.get(key) is None:
            return -2
        exp = self._expires.get(key)
        return -1 if exp is None else max(0, exp - self.now())

    def sweep(self, sample: int = 20, max_rounds: int = 16) -> int:
        """Redis-style active expiry: sample TTL keys, delete the expired ones,
        and repeat while more than 25% of the sample was expired."""
        removed = 0
        for _ in range(max_rounds):
            if not self._expires:
                break
            keys = random.sample(list(self._expires), min(sample, len(self._expires)))
            now = self.now()
            dead = [k for k in keys if self._expires[k] <= now]
            for k in dead:
                self._data.pop(k, None)
                del self._expires[k]
            removed += len(dead)
            if len(dead) * 4 <= len(keys):
                break
        return removed
