import itertools
import random
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from node.evict import record_size
from node.store import Record, Store

KEYSPACE, REQS = 50_000, 200_000
VAL = b"x" * 100
SIZE = record_size(b"k0000000", Record(VAL, (1, "n")))
OUT = Path("docs/img")
OUT.mkdir(parents=True, exist_ok=True)


def key(i: int) -> bytes:
    return f"k{i:07d}".encode()


def zipf_trace(rng: random.Random) -> list[bytes]:
    w = [1 / (r + 1) for r in range(KEYSPACE)]
    return [key(i) for i in rng.choices(range(KEYSPACE), weights=w, k=REQS)]


def scan_trace(rng: random.Random) -> list[bytes]:
    """Zipf traffic plus 10k never-repeated keys every 20k requests."""
    fresh = itertools.count(KEYSPACE)
    out: list[bytes] = []
    for i, k in enumerate(zipf_trace(rng)):
        out.append(k)
        if i % 20_000 == 19_999:
            out.extend(key(next(fresh)) for _ in range(10_000))
    return out


def hit_rate(policy: str, cap_items: int, trace: list[bytes]) -> float:
    s = Store(clock=lambda: 1, grace_ms=10**9, max_bytes=cap_items * SIZE, policy=policy)
    hits = 0
    for i, k in enumerate(trace):
        r = s.get_record(k, touch=True)
        if r is not None and r.value is not None:
            hits += 1
        else:  # read-through cache: a miss loads the value
            s.apply(k, Record(VAL, (i + 1, "n")))
        if i % 50_000 == 49_999:
            s.age()
    return hits / len(trace)


if __name__ == "__main__":
    rng = random.Random(7)
    traces = {"zipf": zipf_trace(rng), "zipf + scans": scan_trace(rng)}
    fracs = [0.01, 0.02, 0.05, 0.10, 0.20]
    fig, axes = plt.subplots(1, 2, figsize=(11, 4), sharey=True)
    for ax, (name, trace) in zip(axes, traces.items()):
        print(f"\n{name}: hit rate by cache size (fraction of keyspace)")
        print(f"{'size':>6} {'LRU':>8} {'LFU':>8}")
        curves = {"lru": [], "lfu": []}
        for f in fracs:
            row = {p: hit_rate(p, int(KEYSPACE * f), trace) for p in curves}
            for p in curves:
                curves[p].append(row[p] * 100)
            print(f"{f:>6.0%} {row['lru']:>8.1%} {row['lfu']:>8.1%}")
        for p, ys in curves.items():
            ax.plot([f * 100 for f in fracs], ys, "o-", label=p.upper())
        ax.set_title(name)
        ax.set_xlabel("cache size (% of keyspace)")
        ax.grid(alpha=0.3)
        ax.legend()
    axes[0].set_ylabel("hit rate (%)")
    plt.savefig(OUT / "eviction_hit_rate.png", dpi=150, bbox_inches="tight")
    print("wrote", OUT / "eviction_hit_rate.png")
