import pytest

from node.store import Store


class FakeClock:
    def __init__(self, t=1_000_000):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, ms):
        self.t += ms


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def store(clock):
    return Store(clock=clock)


def test_set_get_missing(store):
    store.set(b"a", b"1")
    assert store.get(b"a") == b"1"
    assert store.get(b"nope") is None


def test_lazy_expiry_boundary(store, clock):
    store.set(b"a", b"1", expire_at_ms=clock() + 1000)
    clock.advance(999)
    assert store.get(b"a") == b"1"
    clock.advance(1)
    assert store.get(b"a") is None
    assert len(store) == 0


def test_plain_set_clears_ttl(store, clock):
    store.set(b"a", b"1", clock() + 1000)
    store.set(b"a", b"2")
    assert store.ttl_ms(b"a") == -1


def test_ttl_codes(store, clock):
    assert store.ttl_ms(b"x") == -2
    store.set(b"a", b"1")
    assert store.ttl_ms(b"a") == -1
    store.set_expire_at(b"a", clock() + 500)
    assert store.ttl_ms(b"a") == 500


def test_persist(store, clock):
    store.set(b"a", b"1", clock() + 500)
    assert store.persist(b"a") is True
    assert store.persist(b"a") is False
    clock.advance(10_000)
    assert store.get(b"a") == b"1"


def test_delete(store):
    store.set(b"a", b"1")
    assert store.delete(b"a") is True
    assert store.delete(b"a") is False


def test_sweep_removes_expired_without_reads(store, clock):
    for i in range(100):
        store.set(f"k{i}".encode(), b"v", clock() + 10)
    clock.advance(11)
    store.sweep()
    assert len(store) == 0
