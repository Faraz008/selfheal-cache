import random
from dataclasses import replace

import pytest

from node.membership import Entry, Membership, State, supersedes


class FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def advance(self, s):
        self.t += s


def entry(nid, state=State.ALIVE, inc=1):
    return Entry(nid, "h", 7000, 8000, state, inc)


@pytest.fixture
def clock():
    return FakeClock()


@pytest.fixture
def m(clock):
    return Membership(entry("me", inc=100), clock=clock)


def test_unknown_entry_is_adopted(m):
    assert m.merge([entry("a")]) == [entry("a")]
    assert m.get("a").state is State.ALIVE


def test_higher_incarnation_beats_any_state(m):
    m.merge([entry("a", State.DEAD, 5)])
    m.merge([entry("a", State.ALIVE, 6)])
    assert m.get("a").state is State.ALIVE


def test_same_incarnation_worse_state_wins_and_is_sticky(m):
    m.merge([entry("a", State.ALIVE, 5)])
    m.merge([entry("a", State.SUSPECT, 5)])
    assert m.get("a").state is State.SUSPECT
    m.merge([entry("a", State.ALIVE, 5)])  # old news cannot clear a suspicion
    assert m.get("a").state is State.SUSPECT


def test_lower_incarnation_ignored(m):
    m.merge([entry("a", State.ALIVE, 5)])
    assert m.merge([entry("a", State.DEAD, 4)]) == []


def test_merge_is_order_independent():
    rng = random.Random(7)
    pool = [entry(n, rng.choice(list(State)), rng.randint(1, 4)) for n in "abc" for _ in range(10)]
    results = []
    for seed in range(5):
        mm = Membership(entry("me"))
        shuffled = pool[:]
        random.Random(seed).shuffle(shuffled)
        if seed % 2:
            mm.merge(shuffled)  # one big batch
        else:
            for e in shuffled:  # one at a time
                mm.merge([e])
        results.append({e.node_id: e for e in mm.entries()})
    assert all(r == results[0] for r in results)


def test_supersedes_is_a_strict_order():
    a, b = entry("x", State.ALIVE, 3), entry("x", State.SUSPECT, 3)
    assert supersedes(b, a) and not supersedes(a, b) and not supersedes(a, a)


def test_suspicion_about_me_is_refuted(m):
    changed = m.merge([entry("me", State.SUSPECT, 100)])
    assert m.self_entry.state is State.ALIVE and m.self_entry.incarnation == 101
    assert m.self_entry in changed


def test_dead_rumor_about_me_is_refuted_above_it(m):
    m.merge([entry("me", State.DEAD, 150)])
    assert m.self_entry.incarnation == 151 and m.self_entry.state is State.ALIVE


def test_stale_accusation_about_me_ignored(m):
    assert m.merge([entry("me", State.SUSPECT, 99)]) == []
    assert m.self_entry.incarnation == 100


def test_never_adopts_remote_view_of_self(m):
    m.merge([entry("me", State.ALIVE, 500)])
    assert m.self_entry.incarnation == 100


def test_suspect_times_out_to_dead(m, clock):
    m.merge([entry("a")])
    assert m.mark_suspect("a")
    clock.advance(4.9)
    assert m.expire_suspects(5.0) == []
    clock.advance(0.2)
    assert m.expire_suspects(5.0) == ["a"]
    assert m.get("a").state is State.DEAD and m.is_dead("a")


def test_refutation_cancels_the_timer(m, clock):
    m.merge([entry("a", inc=1)])
    m.mark_suspect("a")
    clock.advance(1)
    m.merge([entry("a", State.ALIVE, 2)])
    clock.advance(100)
    assert m.expire_suspects(5.0) == []
    assert m.get("a").state is State.ALIVE


def test_repeated_suspect_gossip_does_not_restart_timer(m, clock):
    m.merge([entry("a", State.SUSPECT, 1)])
    clock.advance(3)
    m.merge([entry("a", State.SUSPECT, 1)])  # not superseding -> ignored
    clock.advance(2)
    assert m.expire_suspects(5.0) == ["a"]


def test_mark_suspect_only_from_alive_and_never_self(m):
    m.merge([entry("a", State.DEAD, 1)])
    assert not m.mark_suspect("a")
    assert not m.mark_suspect("me")
    assert not m.mark_suspect("ghost")


def test_listeners_receive_changes(m):
    seen = []
    m.subscribe(seen.append)
    m.merge([entry("a")])
    m.merge([entry("a")])  # no change, no callback
    assert seen == [[entry("a")]]


def test_read_priority_orders_states(m):
    m.merge(
        [entry("a"), entry("b", State.SUSPECT), entry("c", State.JOINING), entry("d", State.DEAD)]
    )
    order = sorted(["d", "c", "b", "a", "zzz"], key=m.read_priority)
    assert order == ["a", "b", "c", "d", "zzz"]


def test_activate_bumps_incarnation(clock):
    j = Membership(replace(entry("me", inc=10), state=State.JOINING), clock=clock)
    j.activate()
    assert j.self_entry.state is State.ALIVE and j.self_entry.incarnation == 11


def test_left_outranks_dead_and_is_terminal(m):
    m.merge([entry("a", State.DEAD, 5)])
    m.merge([entry("a", State.LEFT, 5)])
    assert m.get("a").state is State.LEFT
    m.merge([entry("a", State.ALIVE, 5), entry("a", State.DEAD, 5)])
    assert m.get("a").state is State.LEFT
    m.merge([entry("a", State.JOINING, 6)])  # same id restarting: a fresh life
    assert m.get("a").state is State.JOINING


def test_left_nodes_leave_the_ring_and_the_peer_list(m):
    m.merge([entry("a"), entry("b", State.LEFT, 3)])
    assert m.ring_node_ids() == ["a", "me"]
    assert [e.node_id for e in m.others()] == ["a"]
    assert m.get("b") is not None  # the tombstone stays so gossip can't resurrect it


def test_leave_lifecycle(m):
    m.begin_leave()
    assert m.self_entry.state is State.LEAVING and m.self_entry.incarnation == 100
    assert m.leaving_ids() == ["me"]
    m.finish_leave()
    assert m.self_entry.state is State.LEFT
    m.cancel_leave()  # too late: no-op
    assert m.self_entry.state is State.LEFT


def test_cancel_leave_returns_to_alive_above_the_rumor(m):
    m.begin_leave()
    m.cancel_leave()
    assert m.self_entry.state is State.ALIVE and m.self_entry.incarnation == 101


def test_leaving_node_does_not_refute_accusations(m):
    m.begin_leave()
    m.merge([entry("me", State.DEAD, 100)])
    assert m.self_entry.state is State.LEAVING


def test_forgetting_a_live_node_is_refuted_by_that_node():
    a = Membership(entry("a", inc=7))
    a.merge([entry("a", State.LEFT, 7)])
    assert a.self_entry.state is State.ALIVE and a.self_entry.incarnation == 8


def test_forget(m):
    m.merge([entry("a", State.DEAD, 2)])
    assert m.forget("a") and m.get("a").state is State.LEFT
    assert not m.forget("me") and not m.forget("ghost")
