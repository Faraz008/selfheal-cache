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


def test_newer_version_wins_regardless_of_arrival_order(store):
    store.apply(b"a", rec(b"new", 20))
    assert store.apply(b"a", rec(b"old", 10)) is False
    assert store.get_live(b"a").value == b"new"


def test_equal_version_is_idempotent(store):
    assert store.apply(b"a", rec(b"1", 10)) is True
    assert store.apply(b"a", rec(b"1", 10)) is False  # a retried replication is harmless


def test_timestamp_tie_broken_by_node_id(clock):
    s1, s2 = Store(clock=clock), Store(clock=clock)
    s1.apply(b"a", rec(b"from-n1", 10, "n1"))
    s1.apply(b"a", rec(b"from-n2", 10, "n2"))
    s2.apply(b"a", rec(b"from-n2", 10, "n2"))
    s2.apply(b"a", rec(b"from-n1", 10, "n1"))
    assert s1.get_live(b"a").value == s2.get_live(b"a").value == b"from-n2"


def test_tombstone_blocks_resurrection(store, clock):
    store.apply(b"a", rec(b"1", 10))
    store.apply(b"a", Record(None, (20, "n1"), clock()))
    assert store.get_live(b"a") is None
    assert store.apply(b"a", rec(b"1", 10)) is False  # stale replica pushes the old value
    assert store.get_live(b"a") is None
    assert store.apply(b"a", rec(b"2", 30)) is True  # a genuinely newer write wins
    assert store.get_live(b"a").value == b"2"


def test_expired_value_also_blocks_resurrection(store, clock):
    t = clock()
    store.apply(b"a", rec(b"new", t, exp=t + 100))
    clock.advance(200)
    assert store.get_live(b"a") is None
    assert store.apply(b"a", rec(b"old", t - 50)) is False


def test_tombstone_collected_after_grace(store, clock):
    t = clock()
    store.apply(b"a", Record(None, (t, "n1"), t))
    store.sweep()
    assert store.raw_len() == 1
    clock.advance(1000)
    store.sweep()
    assert store.raw_len() == 0


def test_already_collectable_record_clears_older_value(store, clock):
    t = clock()
    store.apply(b"a", rec(b"1", 10))
    assert store.apply(b"a", Record(None, (20, "n1"), t - 5000)) is False
    assert store.get_live(b"a") is None
    assert store.raw_len() == 0


def test_version_clock_strictly_increasing_in_same_ms():
    vc = VersionClock("n1", lambda: 5)
    assert [vc.next() for _ in range(3)] == [(5, "n1"), (6, "n1"), (7, "n1")]


def test_version_clock_follows_wall_clock(clock):
    vc = VersionClock("n1", clock)
    a = vc.next()
    clock.advance(100)
    assert vc.next()[0] - a[0] == 100


def test_transferable_ships_expired_value_as_bare_tombstone(store, clock):
    t = clock()
    store.apply(b"a", rec(b"secret", t, exp=t + 100))
    clock.advance(200)
    r = store.transferable(b"a")
    assert r.value is None and r.version == (t, "n1") and r.expire_at == t + 100
    clock.advance(1000)  # now past the grace
    assert store.transferable(b"a") is None


def test_transferable_ships_live_values_untouched(store, clock):
    store.apply(b"a", rec(b"1", 10))
    assert store.transferable(b"a").value == b"1"
    assert store.transferable(b"missing") is None


def test_drop_if_version_only_drops_that_exact_version(store):
    store.apply(b"a", rec(b"1", 10))
    assert store.drop_if_version(b"a", (11, "n1")) is False
    assert store.drop_if_version(b"a", (10, "n1")) is True
    assert store.get_record(b"a") is None


def test_keys_snapshot_is_a_copy(store):
    store.apply(b"a", rec(b"1", 10))
    snap = store.keys_snapshot()
    store.apply(b"b", rec(b"1", 10))
    assert snap == [b"a"]
