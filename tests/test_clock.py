from node.clock import SHIFT, HybridClock, hlc_ms


class Wall:
    def __init__(self, t=1_000_000):
        self.t = t

    def __call__(self):
        return self.t


def test_strictly_increasing_within_one_millisecond():
    c = HybridClock("n1", Wall())
    ts = [c.next()[0] for _ in range(5)]
    assert ts == sorted(set(ts)) and {hlc_ms(t) for t in ts} == {1_000_000}


def test_follows_the_wall_clock():
    w = Wall()
    c = HybridClock("n1", w)
    c.next()
    w.t += 100
    assert hlc_ms(c.next()[0]) == 1_000_100


def test_wall_clock_going_backwards_never_regresses():
    w = Wall()
    c = HybridClock("n1", w)
    a = c.next()
    w.t -= 5000
    assert c.next()[0] > a[0]


def test_a_burst_does_not_run_ahead_of_the_wall_clock():
    w = Wall()
    c = HybridClock("n1", w)
    for _ in range(100_000):  # the old ms-counter clock would drift ~100 s ahead here
        c.next()
    assert hlc_ms(c.next()[0]) - w.t <= 2


def test_observe_pulls_the_clock_past_a_remote_timestamp():
    w = Wall()
    c = HybridClock("n1", w)
    remote = (w.t + 2000) << SHIFT
    assert c.observe(remote) is True
    assert c.next()[0] > remote


def test_observe_of_an_older_timestamp_changes_nothing():
    c = HybridClock("n1", Wall())
    a = c.next()
    assert c.observe(1) is True
    assert c.next()[0] > a[0]


def test_observe_refuses_absurd_future_timestamps():
    w = Wall()
    c = HybridClock("n1", w, max_drift_ms=1000)
    assert c.observe((w.t + 1000) << SHIFT) is True  # exactly at the limit: fine
    assert c.observe((w.t + 5000) << SHIFT) is False
    assert c.rejected == 1


def test_ties_break_by_node_id():
    w = Wall()
    assert HybridClock("n1", w).next() < HybridClock("n2", w).next()


def test_hlc_ms_roundtrip():
    assert hlc_ms((123 << SHIFT) | 7) == 123
