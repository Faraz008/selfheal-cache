import logging

from .store import Version, now_ms

log = logging.getLogger("clock")

SHIFT = 16  # 65,536 events per millisecond before the counter spills into the ms bits


def hlc_ms(ts: int) -> int:
    """Physical milliseconds encoded in an HLC value."""
    return ts >> SHIFT


class HybridClock:
    """Version source. ts = (physical_ms << 16) | counter, so ordering is lexicographic
    on (ms, counter). It follows the wall clock but can never go backwards, and
    observe() pulls it ahead of anything we have seen from another node, so a write
    made AFTER seeing a version always gets a LARGER one, whatever the wall clocks say."""

    def __init__(self, node_id: str, wall=now_ms, max_drift_ms: int = 5000):
        self.node_id, self._wall, self.max_drift_ms = node_id, wall, max_drift_ms
        self._last = 0
        self.rejected = 0

    @property
    def last(self) -> int:
        return self._last

    def next(self) -> Version:
        self._last = max(self._wall() << SHIFT, self._last + 1)
        return (self._last, self.node_id)

    def observe(self, ts: int) -> bool:
        """Merge a remote timestamp. False if it was refused as absurdly far ahead."""
        if ts <= self._last:
            return True
        if ts > (self._wall() + self.max_drift_ms) << SHIFT:
            self.rejected += 1
            if self.rejected % 1000 == 1:  # don't flood the log
                log.warning(
                    "refusing HLC %d ms ahead of our wall clock (>%d allowed)",
                    hlc_ms(ts) - self._wall(),
                    self.max_drift_ms,
                )
            return False
        self._last = ts
        return True
