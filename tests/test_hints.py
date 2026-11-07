from node.hints import HintStore
from node.store import Record


class Clock:
    def __init__(self):
        self.t = 1000

    def __call__(self):
        return self.t


def rec(ts, v=b"x"):
    return Record(v, (ts, "n1"))


def test_add_and_pending():
    h = HintStore(10, 1000, Clock())
    assert h.add("n2", b"a", rec(1)) and h.count == 1
    assert h.pending("n2", 10) == [(b"a", rec(1))]
    assert h.pending("n9", 10) == []


def test_newer_replaces_older_and_older_is_ignored():
    h = HintStore(10, 1000, Clock())
    h.add("n2", b"a", rec(5, b"new"))
    h.add("n2", b"a", rec(3, b"old"))
    assert h.count == 1 and h.pending("n2", 1)[0][1].value == b"new"
    h.add("n2", b"a", rec(9, b"newest"))
    assert h.count == 1 and h.pending("n2", 1)[0][1].value == b"newest"


def test_cap_rejects_new_keys_but_still_updates_existing():
    h = HintStore(2, 1000, Clock())
    assert h.add("n2", b"a", rec(1)) and h.add("n2", b"b", rec(1))
    assert h.add("n2", b"c", rec(1)) is False and h.rejected == 1
    assert h.add("n2", b"a", rec(2)) is True  # update, not growth
    assert h.count == 2


def test_done_keeps_a_newer_hint_that_arrived_meanwhile():
    h = HintStore(10, 1000, Clock())
    h.add("n2", b"a", rec(1))
    h.add("n2", b"a", rec(2))
    h.done("n2", b"a", rec(1))  # delivered the OLD one
    assert h.count == 1
    h.done("n2", b"a", rec(2))
    assert h.count == 0 and h.owners() == []


def test_ttl_expiry():
    c = Clock()
    h = HintStore(10, 500, c)
    h.add("n2", b"a", rec(1))
    c.t += 499
    assert h.expire() == 0
    c.t += 1
    assert h.expire() == 1 and h.count == 0
