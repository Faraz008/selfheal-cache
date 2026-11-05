import pytest

from node.store import Record, Store, VersionClock


class FakeClock:
    def __init__(self, t=1_000_000):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, ms):
        self.t += ms


def rec(value, ts, node="n1", exp=None):
    return Record(value, (ts, node), exp)


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def store(clock):
    return Store(clock=clock, grace_ms=1000)


def test_apply_and_get_live(store):
    assert store.apply(b"a", rec(b"1", 10)) is True
    assert store.get_live(b"a").value == b"1"
    assert store.get_live(b"nope") is None
    assert len(store) == 1


def test_lazy_expiry_boundary(store, clock):
    t = clock()
    store.apply(b"a", rec(b"1", t, exp=t + 1000))
    clock.advance(999)
    assert store.get_live(b"a") is not None
    clock.advance(1)
    assert store.get_live(b"a") is None
    assert len(store) == 0
    assert store.raw_len() == 1  # invisible, but kept so old versions can't resurrect it


def test_sweep_collects_only_after_grace(store, clock):
    t = clock()
    for i in range(100):
        store.apply(f"k{i}".encode(), rec(b"v", t, exp=t + 100))
    clock.advance(500)  # expired, still inside the 1000 ms grace
    store.sweep()
    assert store.raw_len() == 100
    clock.advance(700)  # now past expire_at + grace
    store.sweep()
    assert store.raw_len() == 0
