import random

import pytest

from node.evict import record_size
from node.store import KeyIndex, Record, Store

from .test_store import FakeClock, rec

VAL = b"x" * 100


def sz(key: bytes = b"k0") -> int:
    return record_size(key, Record(VAL, (1, "n1")))


@pytest.fixture
def clock():
    return FakeClock()


def make(clock, items=5, policy="lru", extra=0):
    return Store(clock=clock, grace_ms=10**9, max_bytes=items * sz() + extra, policy=policy)


def test_overwrite_replaces_accounting(clock):
    s = Store(clock=clock)
    s.apply(b"a", rec(b"x" * 100, 1))
    m = s.mem
    s.apply(b"a", rec(b"y" * 100, 2))
    assert s.mem == m  # no double counting
    s.apply(b"a", Record(None, (3, "n1"), clock()))  # tombstone releases the payload
    assert s.mem == record_size(b"a", Record(None, (3, "n1")))
    s._remove(b"a")
    assert s.mem == 0


def test_no_cap_means_no_eviction(clock):
    s = Store(clock=clock)
    for i in range(100):
        s.apply(f"k{i}".encode(), rec(VAL, i + 1))
    assert s.evictions == 0 and len(s) == 100


def test_lru_evicts_the_coldest_key(clock):
    s = make(clock)
    for i in range(5):
        s.apply(f"k{i}".encode(), rec(VAL, 10 + i))
    s.get_record(b"k0", touch=True)  # k0 becomes the hottest
    s.apply(b"k5", rec(VAL, 20))
    assert s.get_record(b"k1") is None  # the coldest went
    assert s.get_record(b"k0") is not None
    assert s.evictions == 1 and s.mem <= s.max_bytes


def test_eviction_writes_no_tombstone(clock):
    s = make(clock, 2)
    for i in range(4):
        s.apply(f"k{i}".encode(), rec(VAL, 10 + i))
    assert s.get_record(b"k0") is None  # truly gone: a miss, not a delete
    assert s.raw_len() == 2


def test_the_key_just_written_is_never_its_own_victim(clock):
    s = Store(clock=clock, max_bytes=10, policy="lru")  # smaller than one record
    assert s.apply(b"a", rec(VAL, 1)) is True
    assert s.get_live(b"a") is not None
    assert s.evict_blocked == 1 and s.mem > s.max_bytes  # soft cap


def test_tombstones_are_never_evicted(clock):
    s = make(clock, 2)
    for i in range(3):
        s.apply(f"t{i}".encode(), Record(None, (10 + i, "n1"), clock()))
    for i in range(6):
        s.apply(f"k{i}".encode(), rec(VAL, 100 + i))
    for i in range(3):
        assert s.get_record(f"t{i}".encode()).is_tombstone  # resurrection stays blocked
    assert s.evict_blocked >= 1  # the cap overshoots rather than drop a tombstone


def test_expired_values_shrink_instead_of_evicting_anything(clock):
    tomb = record_size(b"e0", Record(None, (1, "n1")))
    s = make(clock, 3, extra=tomb)  # exactly enough room once e0's payload is reclaimed
    t = clock()
    s.apply(b"e0", rec(VAL, 10, exp=t + 100))
    s.apply(b"k1", rec(VAL, 11))
    s.apply(b"k2", rec(VAL, 12))
    clock.advance(200)  # e0 expired, still inside the grace
    s.apply(b"k3", rec(VAL, 13))
    assert s.evictions == 0 and s.shrunk == 1
    e0 = s.get_record(b"e0")
    assert e0.value is None and e0.version == (10, "n1") and e0.expire_at == t + 100
    assert s.apply(b"e0", rec(b"old", 5)) is False  # still blocks resurrection
    assert all(s.get_live(k) is not None for k in (b"k1", b"k2", b"k3"))


def test_sweep_shrinks_expired_payload_before_the_grace_ends(clock):
    s = Store(clock=clock, grace_ms=10_000)
    t = clock()
    s.apply(b"a", rec(b"x" * 1000, 1, exp=t + 100))
    before = s.mem
    clock.advance(500)
    s.sweep()
    assert s.mem < before and s.get_record(b"a").value is None
    assert s.raw_len() == 1  # the tombstone-equivalent stays until the grace passes


def test_removal_by_any_path_keeps_accounting_exact(clock):
    s = make(clock, 3)
    for i in range(10):
        s.apply(f"k{i}".encode(), rec(VAL, 10 + i))
    for k in s.keys_snapshot():
        s.drop_if_version(k, s.get_record(k).version)
    assert s.mem == 0 and s.raw_len() == 0


def test_key_index_matches_a_plain_set():
    idx, ref, rng = KeyIndex(), set(), random.Random(1)
    for _ in range(5000):
        k = bytes([rng.randrange(50)])
        if rng.random() < 0.5:
            idx.add(k)
            ref.add(k)
        else:
            idx.discard(k)
            ref.discard(k)
        assert len(idx) == len(ref)
    assert set(idx.sample(1000)) == ref
    assert all(k in idx for k in ref)
