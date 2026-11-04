from collections import Counter

import pytest

from node.ring import Ring


def keys(n: int) -> list[bytes]:
    return [f"key:{i}".encode() for i in range(n)]


def make_ring(n: int, vnodes: int = 150) -> Ring:
    r = Ring(vnodes)
    for i in range(1, n + 1):
        r.add_node(f"n{i}")
    return r


def test_empty_ring():
    r = Ring()
    assert r.primary(b"k") is None
    assert r.get_preference_list(b"k", 3) == []


def test_invalid_vnodes():
    with pytest.raises(ValueError):
        Ring(0)


def test_add_remove_idempotent():
    r = make_ring(3)
    r.add_node("n1")
    assert len(r) == 3
    r.remove_node("n9")
    assert len(r) == 3
    r.remove_node("n1")
    assert r.nodes == ["n2", "n3"]


def test_insertion_order_does_not_matter():
    a, b = Ring(), Ring()
    for n in ["n1", "n2", "n3", "n4"]:
        a.add_node(n)
    for n in ["n4", "n2", "n1", "n3"]:
        b.add_node(n)
    assert all(a.primary(k) == b.primary(k) for k in keys(2000))


def test_deterministic_across_instances():
    assert make_ring(5).primary(b"hello") == make_ring(5).primary(b"hello")


def test_distribution_is_balanced():
    r = make_ring(5)
    counts = Counter(r.primary(k) for k in keys(50_000))
    assert len(counts) == 5
    mean = 50_000 / 5
    assert all(abs(c - mean) / mean < 0.30 for c in counts.values())


def test_adding_node_moves_about_one_over_n():
    old, new = make_ring(4), make_ring(5)
    ks = keys(20_000)
    moved = [k for k in ks if old.primary(k) != new.primary(k)]
    assert 0.12 < len(moved) / len(ks) < 0.28  # ideal: 1/5 = 0.20
    # The key property: keys ONLY move to the new node, never between old ones.
    assert all(new.primary(k) == "n5" for k in moved)


def test_removing_node_only_moves_its_keys():
    full = make_ring(5)
    less = make_ring(5)
    less.remove_node("n3")
    for k in keys(10_000):
        if full.primary(k) != "n3":
            assert less.primary(k) == full.primary(k)
        else:
            assert less.primary(k) != "n3"


def test_preference_list_distinct_and_sized():
    r = make_ring(5)
    for k in keys(2000):
        pl = r.get_preference_list(k, 3)
        assert len(pl) == 3
        assert len(set(pl)) == 3


def test_first_of_preference_list_is_primary():
    r = make_ring(5)
    for k in keys(1000):
        assert r.get_preference_list(k, 3)[0] == r.primary(k)


def test_preference_list_larger_than_cluster():
    r = make_ring(2)
    assert sorted(r.get_preference_list(b"k", 5)) == ["n1", "n2"]
    assert make_ring(1).get_preference_list(b"k", 3) == ["n1"]


def test_preference_list_wraparound_tiny_ring():
    # 1 vnode per node: many keys hash past the last token and must wrap.
    r = make_ring(3, vnodes=1)
    for k in keys(1000):
        pl = r.get_preference_list(k, 3)
        assert sorted(pl) == ["n1", "n2", "n3"]


def test_str_and_bytes_keys_equivalent():
    r = make_ring(4)
    assert r.get_preference_list("abc", 2) == r.get_preference_list(b"abc", 2)


def test_removal_leaves_unrelated_lists_unchanged():
    full, less = make_ring(5), make_ring(5)
    less.remove_node("n2")
    for k in keys(5000):
        pl = full.get_preference_list(k, 3)
        if "n2" not in pl:
            assert less.get_preference_list(k, 3) == pl


def test_replica_load_is_balanced():
    r = make_ring(5)
    counts = Counter(n for k in keys(30_000) for n in r.get_preference_list(k, 3))
    mean = 30_000 * 3 / 5
    assert all(abs(c - mean) / mean < 0.25 for c in counts.values())
