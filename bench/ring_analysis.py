import statistics
import time
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless, no display needed
import matplotlib.pyplot as plt

from node.ring import Ring, hash64

OUT = Path("docs/img")
OUT.mkdir(parents=True, exist_ok=True)
KEYS = [f"key:{i}".encode() for i in range(100_000)]
KEY_HASHES = [hash64(k) for k in KEYS]


def make_ring(n: int, vnodes: int = 150) -> Ring:
    r = Ring(vnodes)
    for i in range(1, n + 1):
        r.add_node(f"n{i}")
    return r


def ring_moved(n: int) -> float:
    a, b = make_ring(n), make_ring(n + 1)
    return sum(a.primary(k) != b.primary(k) for k in KEYS) / len(KEYS)


def modulo_moved(n: int) -> float:
    return sum(h % n != h % (n + 1) for h in KEY_HASHES) / len(KEYS)


def cv(counts: list[int]) -> float:
    """Coefficient of variation: stddev / mean. Lower = more balanced."""
    return statistics.pstdev(counts) / statistics.mean(counts)


def experiment_movement():
    sizes = list(range(2, 11))
    ring = [ring_moved(n) * 100 for n in sizes]
    mod = [modulo_moved(n) * 100 for n in sizes]
    ideal = [100 / (n + 1) for n in sizes]
    print("\nKeys moved when going N -> N+1 nodes")
    print(f"{'N':>3} {'modulo':>9} {'ring':>9} {'ideal':>9}")
    for n, m, r, i in zip(sizes, mod, ring, ideal, strict=True):
        print(f"{n:>3} {m:>8.1f}% {r:>8.1f}% {i:>8.1f}%")
    plt.figure(figsize=(8, 5))
    plt.plot(sizes, mod, "o-", label="modulo hashing")
    plt.plot(sizes, ring, "s-", label="consistent hashing (150 vnodes)")
    plt.plot(sizes, ideal, "k--", label="ideal 1/(N+1)")
    plt.xlabel("nodes before adding one")
    plt.ylabel("% of keys that change owner")
    plt.title("Key movement when adding a node")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.savefig(OUT / "key_movement.png", dpi=150, bbox_inches="tight")
    plt.close()


def experiment_vnodes():
    vcounts = [1, 5, 10, 25, 50, 100, 150, 200, 500]
    cvs, build_ms, lookup_us = [], [], []
    print("\n10 nodes, 100k keys")
    print(f"{'vnodes':>7} {'CV %':>7} {'max/mean':>9} {'build ms':>9} {'lookup us':>10}")
    for v in vcounts:
        t = time.perf_counter()
        ring = make_ring(10, v)
        b = (time.perf_counter() - t) * 1000
        t = time.perf_counter()
        owners = [ring.primary(k) for k in KEYS]
        lk = (time.perf_counter() - t) / len(KEYS) * 1e6
        counts = [owners.count(f"n{i}") for i in range(1, 11)]
        c = cv(counts) * 100
        cvs.append(c)
        build_ms.append(b)
        lookup_us.append(lk)
        print(
            f"{v:>7} {c:>6.1f}% {max(counts) / statistics.mean(counts):>9.2f} {b:>9.1f} {lk:>10.2f}"
        )
    plt.figure(figsize=(8, 5))
    plt.plot(vcounts, cvs, "o-")
    plt.xscale("log")
    plt.xlabel("virtual nodes per physical node (log scale)")
    plt.ylabel("load imbalance (CV %)")
    plt.title("More vnodes = more even key distribution")
    plt.grid(alpha=0.3)
    plt.savefig(OUT / "vnode_balance.png", dpi=150, bbox_inches="tight")
    plt.close()


def experiment_distribution():
    ring = make_ring(5)
    owners = [ring.primary(k) for k in KEYS]
    names = ring.nodes
    counts = [owners.count(n) for n in names]
    plt.figure(figsize=(7, 4))
    plt.bar(names, counts)
    plt.axhline(len(KEYS) / 5, color="k", ls="--", label="perfect balance")
    plt.ylabel("keys owned")
    plt.title("100k keys across 5 nodes (150 vnodes)")
    plt.legend()
    plt.savefig(OUT / "distribution.png", dpi=150, bbox_inches="tight")
    plt.close()


if __name__ == "__main__":
    experiment_movement()
    experiment_vnodes()
    experiment_distribution()
    print(f"\ncharts written to {OUT}/")
