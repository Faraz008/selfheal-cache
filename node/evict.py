from collections import OrderedDict

OVERHEAD = 261  # bytes charged per record beyond key+value; calibrated on Day 35
MAX_FREQ = 255  # saturating counter: bounds the number of distinct frequency buckets
LFU_INIT = 3  # new keys start above 1 so they get a chance before being evicted


def record_size(key: bytes, rec) -> int:
    return len(key) + (0 if rec.value is None else len(rec.value)) + OVERHEAD


class LRU:
    """Evicts the least recently used key. OrderedDict: coldest first, O(1) everything."""

    name = "lru"

    def __init__(self):
        self._d: OrderedDict[bytes, None] = OrderedDict()

    def __len__(self) -> int:
        return len(self._d)

    def add(self, key: bytes) -> None:  # new or re-written: counts as an access
        self._d[key] = None
        self._d.move_to_end(key)

    def touch(self, key: bytes) -> None:
        if key in self._d:
            self._d.move_to_end(key)

    def remove(self, key: bytes) -> None:
        self._d.pop(key, None)

    def victim(self, protect: bytes | None = None) -> bytes | None:
        for k in self._d:
            if k != protect:
                return k
        return None

    def age(self) -> None:
        pass


class LFU:
    """Evicts the least frequently used key, least recently used among ties.
    Frequency buckets (freq -> OrderedDict of keys), counters saturate at MAX_FREQ,
    and age() halves everything so yesterday's hot keys don't live forever."""

    name = "lfu"

    def __init__(self):
        self._freq: dict[bytes, int] = {}
        self._buckets: dict[int, OrderedDict[bytes, None]] = {}

    def __len__(self) -> int:
        return len(self._freq)

    def freq(self, key: bytes) -> int:
        return self._freq.get(key, 0)

    def _place(self, key: bytes, f: int) -> None:
        self._freq[key] = f
        self._buckets.setdefault(f, OrderedDict())[key] = None

    def _unplace(self, key: bytes) -> int:
        f = self._freq.pop(key)
        bucket = self._buckets[f]
        del bucket[key]
        if not bucket:
            del self._buckets[f]
        return f

    def add(self, key: bytes) -> None:
        if key in self._freq:
            self.touch(key)  # an overwrite is an access
        else:
            self._place(key, LFU_INIT)

    def touch(self, key: bytes) -> None:
        if key in self._freq:
            self._place(key, min(self._unplace(key) + 1, MAX_FREQ))

    def remove(self, key: bytes) -> None:
        if key in self._freq:
            self._unplace(key)

    def victim(self, protect: bytes | None = None) -> bytes | None:
        for f in sorted(self._buckets):  # at most MAX_FREQ buckets
            for k in self._buckets[f]:
                if k != protect:
                    return k
        return None

    def age(self) -> None:
        """O(n). Called from the sweeper every lfu_decay_ms, so a very large store
        pays a short pause once a minute."""
        old = self._buckets
        self._freq, self._buckets = {}, {}
        for f in sorted(old):
            for k in old[f]:
                self._place(k, max(1, f // 2))


def make_policy(name: str):
    return {"lru": LRU, "lfu": LFU}[name]()
