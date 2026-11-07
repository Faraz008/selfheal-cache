import hashlib
import itertools
from bisect import bisect_right


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

    def walk(self, key: bytes | str):
        """Yield every distinct physical node once, clockwise from the key.
        The first N are the preference list; the rest are stand-in candidates."""
        if not self._hashes:
            return
        if isinstance(key, str):
            key = key.encode()
        size = len(self._hashes)
        start = bisect_right(self._hashes, hash64(key))
        seen: set[str] = set()
        for i in range(size):
            owner = self._owners[(start + i) % size]
            if owner not in seen:
                seen.add(owner)
                yield owner
                if len(seen) == len(self._nodes):
                    return

    def get_preference_list(self, key: bytes | str, n: int) -> list[str]:
        if n < 1:
            return []
        return list(itertools.islice(self.walk(key), n))
