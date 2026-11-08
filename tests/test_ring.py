from collections import Counter

import pytest

from node.ring import MAX_HASH, RangeSet, Ring, hash64, ring_diff


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


def test_walk_yields_every_node_once_and_extends_preference_list():
    r = make_ring(5)
    for k in keys(500):
        w = list(r.walk(k))
        assert sorted(w) == ["n1", "n2", "n3", "n4", "n5"]
        assert w[:3] == r.get_preference_list(k, 3)


def test_walk_on_empty_ring():
    assert list(Ring().walk(b"k")) == []


def test_diff_of_identical_rings_is_empty():
    assert ring_diff(make_ring(4), make_ring(4), 3) == []


def test_diff_ranges_are_sorted_disjoint_and_in_bounds():
    moves = ring_diff(make_ring(4), make_ring(5), 3)
    assert moves
    prev = 0
    for mv in moves:
        assert prev <= mv.lo < mv.hi <= MAX_HASH
        prev = mv.hi


def test_adding_a_node_means_only_the_new_node_gains():
    for mv in ring_diff(make_ring(4), make_ring(5), 3):
        assert mv.gained == ("n5",)
        assert len(mv.lost) == 1


def test_removing_a_node_means_only_that_node_loses():
    full, less = make_ring(5), make_ring(5)
    less.remove_node("n3")
    moves = ring_diff(full, less, 3)
    assert moves
    assert all(mv.lost == ("n3",) and len(mv.gained) == 1 for mv in moves)


def test_diff_agrees_with_per_key_lookup():
    old, new = make_ring(4), make_ring(5)
    rs = RangeSet([(m.lo, m.hi) for m in ring_diff(old, new, 3)])
    for k in keys(5000):
        changed = set(old.get_preference_list(k, 3)) != set(new.get_preference_list(k, 3))
        assert (hash64(k) in rs) == changed


def test_diff_from_empty_ring_gains_everything():
    moves = ring_diff(Ring(), make_ring(3), 2)
    assert moves[0].lo == 0 and moves[-1].hi == MAX_HASH
    assert all(mv.old == () and len(mv.new) == 2 for mv in moves)


def test_fraction_of_keyspace_moved_when_adding_a_node():
    moves = ring_diff(make_ring(4), make_ring(5), 3)
    frac = sum(m.hi - m.lo for m in moves) / MAX_HASH
    assert 0.45 < frac < 0.75  # ideal: the new node joins 3/5 of all replica sets
