import hashlib
import itertools
from bisect import bisect_right
from dataclasses import dataclass, replace

MAX_HASH = 1 << 64


def hash64(data: bytes) -> int:
    """Stable 64-bit hash. Identical on every process, machine and run."""
    return int.from_bytes(hashlib.md5(data, usedforsecurity=False).digest()[:8], "big")


class Ring:
    def __init__(self, vnodes: int = 150):
        if vnodes < 1:
            raise ValueError("vnodes must be >= 1")
        self.vnodes = vnodes
        self._nodes: set[str] = set()
        # Parallel sorted arrays: token hash -> owning node id
        self._hashes: list[int] = []
        self._owners: list[str] = []

    # ---- membership ----
    def __len__(self) -> int:
        return len(self._nodes)

    def __contains__(self, node_id: str) -> bool:
        return node_id in self._nodes

    @property
    def nodes(self) -> list[str]:
        return sorted(self._nodes)

    def add_node(self, node_id: str) -> None:
        if node_id not in self._nodes:
            self._nodes.add(node_id)
            self._rebuild()

    def remove_node(self, node_id: str) -> None:
        if node_id in self._nodes:
            self._nodes.discard(node_id)
            self._rebuild()

    def set_nodes(self, node_ids) -> None:
        """Replace membership in one rebuild (used by gossip in Phase 4)."""
        self._nodes = set(node_ids)
        self._rebuild()

    def _rebuild(self) -> None:
        # Sorting by (hash, node_id) makes the ring a pure function of the
        # node SET: insertion order can never change it, even on a hash tie.
        tokens = sorted(
            (hash64(f"{nid}#{i}".encode()), nid) for nid in self._nodes for i in range(self.vnodes)
        )
        self._hashes = [h for h, _ in tokens]
        self._owners = [n for _, n in tokens]

    # ---- lookup ----
    def primary(self, key: bytes | str) -> str | None:
        owners = self.get_preference_list(key, 1)
        return owners[0] if owners else None

    def _walk_from(self, h: int):
        """Yield every distinct physical node once, clockwise from hash h."""
        if not self._hashes:
            return
        size = len(self._hashes)
        start = bisect_right(self._hashes, h)
        seen: set[str] = set()
        for i in range(size):
            owner = self._owners[(start + i) % size]
            if owner not in seen:
                seen.add(owner)
                yield owner
                if len(seen) == len(self._nodes):
                    return

    def walk(self, key: bytes | str):
        if isinstance(key, str):
            key = key.encode()
        return self._walk_from(hash64(key))

    def prefs_at(self, h: int, n: int) -> tuple[str, ...]:
        """Preference list for a raw hash value (used by range diffs)."""
        return tuple(itertools.islice(self._walk_from(h), max(n, 0)))

    def tokens(self) -> list[int]:
        return list(self._hashes)

    def get_preference_list(self, key: bytes | str, n: int) -> list[str]:
        if n < 1:
            return []
        return list(itertools.islice(self.walk(key), n))


@dataclass(frozen=True)
class Move:
    """Keys with hash in [lo, hi) change replica set from `old` to `new`."""

    lo: int
    hi: int
    old: tuple[str, ...]
    new: tuple[str, ...]

    @property
    def gained(self) -> tuple[str, ...]:
        return tuple(n for n in self.new if n not in self.old)

    @property
    def lost(self) -> tuple[str, ...]:
        return tuple(n for n in self.old if n not in self.new)


def ring_diff(old: Ring, new: Ring, n: int) -> list[Move]:
    """Every hash range whose SET of n replicas differs between the two rings.
    Result is sorted, disjoint, and within [0, 2**64)."""
    edges = sorted({0, MAX_HASH, *old.tokens(), *new.tokens()})
    moves: list[Move] = []
    for lo, hi in zip(edges, edges[1:]):
        o, w = old.prefs_at(lo, n), new.prefs_at(lo, n)
        if set(o) == set(w):
            continue
        last = moves[-1] if moves else None
        if last is not None and last.hi == lo and last.old == o and last.new == w:
            moves[-1] = replace(last, hi=hi)  # contiguous and identical: merge
        else:
            moves.append(Move(lo, hi, o, w))
    return moves


class RangeSet:
    """Membership test for sorted, DISJOINT [lo, hi) ranges (ring_diff output qualifies)."""

    def __init__(self, ranges):
        rs = sorted(ranges)
        self.los = [lo for lo, _ in rs]
        self.his = [hi for _, hi in rs]

    def __contains__(self, h: int) -> bool:
        i = bisect_right(self.los, h) - 1
        return i >= 0 and h < self.his[i]
