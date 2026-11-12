import argparse
import asyncio
import random
import sys
import time

from .common import all_see, host_port, info, make_client, raw, sh, view, wait_for
from .workload import Workload

SCENARIOS: dict = {}  # name -> (fn, strict)


def scenario(name: str, strict: bool = True):
    def deco(fn):
        SCENARIOS[name] = (fn, strict)
        return fn

    return deco


class Cluster:
    def __init__(self):
        self.live = [f"n{i}" for i in range(1, 6)]

    def others(self, *nodes: str) -> list[str]:
        return [n for n in self.live if n not in nodes]

    async def wait_alive(self, timeout: float = 90) -> float:
        async def cond():
            for n in self.live:
                v = await view(n)
                if v is None or any(v.get(m) != "alive" for m in self.live):
                    return False
            return True

        return await wait_for(cond, timeout, "every node to see every live node ALIVE")

    async def settle(self, timeout: float = 90) -> None:
        await self.wait_alive(timeout)

        async def drained():
            for n in self.live:
                i = await info(n)
                if i is None or int(i.get("hints_held", 1)) != 0:
                    return False
            return True

        await wait_for(drained, timeout, "hints to drain")
        await asyncio.sleep(3)


class Harness:
    def __init__(self, workload: Workload, seed: int):
        self.w, self.c, self.rng = workload, Cluster(), random.Random(seed)
        self.rows: list[dict] = []

    async def run(self, name: str) -> bool:
        fn, strict = SCENARIOS[name]
        snap, t0 = self.w.snap(), time.monotonic() - self.w.t0
        self.w.mark(f"== {name} ==")
        extra = await fn(self)
        await self.c.settle()
        await self.w.pause()
        checked, bad = await self.w.verify()
        d = self.w.delta(snap)
        t1 = time.monotonic() - self.w.t0
        self.w.resume()
        ok = not bad and (d["failed"] == 0 or not strict)
        row = dict(
            name=name,
            strict=strict,
            checked=checked,
            lost=len(bad),
            p99=self.w.p99_between(t0, t1),
            ok=ok,
            extra=extra,
            bad=bad[:3],
            **d,
        )
        self.rows.append(row)
        print(f"\n--- {name}: {'PASS' if ok else 'FAIL'}{'' if strict else ' (informational)'} ---")
        print(
            f"    {d['ops']} ops, failed={d['failed']}, retries={d['retries']}, "
            f"stale_reads={d['stale']}, worst p99={row['p99']:.0f}ms"
        )
        print(f"    verified {checked} keys, wrong={len(bad)}  {extra}")
        for m in bad[:3]:
            print("      ", m)
        return ok


# ------------------------------------------------------------------ scenarios
@scenario("kill_one")
async def kill_one(h: Harness) -> dict:
    victim = h.rng.choice(h.c.live)
    h.w.mark(f"KILL {victim}")
    await sh("kill", victim)
    detect = await wait_for(all_see(h.c.others(victim), victim, "dead"), 60, f"{victim} DEAD")
    await asyncio.sleep(3)
    h.w.mark(f"START {victim}")
    await sh("start", victim)
    recover = await wait_for(all_see(h.c.live, victim, "alive"), 90, f"{victim} ALIVE")
    return {"victim": victim, "detect_s": round(detect, 1), "recover_s": round(recover, 1)}


@scenario("pause_one")
async def pause_one(h: Harness) -> dict:
    """A frozen process: sockets stay open, nothing answers. Nastier than a crash."""
    victim = h.rng.choice(h.c.live)
    h.w.mark(f"PAUSE {victim}")
    await sh("pause", victim)
    detect = await wait_for(all_see(h.c.others(victim), victim, "dead"), 60, f"{victim} DEAD")
    await asyncio.sleep(2)
    h.w.mark(f"UNPAUSE {victim}")
    await sh("unpause", victim)
    recover = await wait_for(all_see(h.c.live, victim, "alive"), 90, f"{victim} refuting")
    return {"victim": victim, "detect_s": round(detect, 1), "recover_s": round(recover, 1)}


@scenario("kill_two", strict=False)  # beyond what W=2 promises to hide: report, don't fail
async def kill_two(h: Harness) -> dict:
    a, b = h.rng.sample(h.c.live, 2)
    h.w.mark(f"KILL {a} + {b}")
    await sh("kill", a, b)
    detect = await wait_for(all_see(h.c.others(a, b), [a, b], "dead"), 60, "both DEAD")
    await asyncio.sleep(3)
    h.w.mark(f"START {a} + {b}")
    await sh("start", a, b)
    recover = await wait_for(all_see(h.c.live, [a, b], "alive"), 120, "both ALIVE")
    return {"victims": [a, b], "detect_s": round(detect, 1), "recover_s": round(recover, 1)}


@scenario("grow")
async def grow(h: Harness) -> dict:
    h.w.mark("ADD n6")
    t = time.monotonic()
    await sh("add", "n6")  # returns when n6 is healthy, i.e. ALIVE
    h.c.live.append("n6")
    return {"join_s": round(time.monotonic() - t, 1)}


@scenario("shrink")
async def shrink(h: Harness) -> dict:
    victim = h.rng.choice(h.c.live)
    h.w.mark(f"LEAVE {victim}")
    t = time.monotonic()
    reply = await raw(host_port(victim), "CLUSTER", "LEAVE", timeout=150)
    assert reply == "OK", f"CLUSTER LEAVE on {victim} returned {reply!r}"
    took = time.monotonic() - t
    h.c.live.remove(victim)
    await wait_for(all_see(h.c.live, victim, "left"), 30, f"{victim} LEFT")
    return {"victim": victim, "leave_s": round(took, 1)}


# ------------------------------------------------------------------ main
async def amain(default_names: list[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--scenarios", default=",".join(default_names))
    ap.add_argument("--rounds", type=int, default=1)
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--seed", type=int, default=1)
    ap.add_argument("--no-up", action="store_true", help="cluster is already running")
    ap.add_argument("--csv", default="docs/chaos_run.csv")
    a = ap.parse_args()
    names = [n for n in a.scenarios.split(",") if n] * a.rounds

    if not a.no_up:
        await sh("up")
    client = make_client(retries=1, request_timeout=3.0)
    await client.start()
    print("consistency mode:", (await info("n1") or {}).get("consistency", "?"))
    w = Workload(client, workers=a.workers, seed=a.seed)
    h = Harness(w, a.seed)
    await w.start()
    try:
        await asyncio.sleep(5)  # warm-up
        await h.c.settle()
        for n in names:
            await h.run(n)
    finally:
        await w.stop()
        if w.rows:
            w.write_files(a.csv)
        await client.close()

    print("\n=========== SUMMARY ===========")
    for r in h.rows:
        print(
            f"{'PASS' if r['ok'] else 'FAIL':5} "
            f"{r['name']:11} failed={r['failed']} lost={r['lost']}"
            f" retries={r['retries']} stale={r['stale']} "
            f"p99={r['p99']:.0f}ms {r['extra']}"
        )
    failed = [r for r in h.rows if not r["ok"]]
    print("ALL GREEN" if not failed else f"{len(failed)} scenario(s) FAILED")
    return 1 if failed else 0


def main(default_names: list[str]) -> None:
    sys.exit(asyncio.run(amain(default_names)))


if __name__ == "__main__":
    main(["kill_one", "pause_one", "kill_two", "grow", "shrink", "kill_one"])
