import asyncio
import time
from collections import Counter

from .common import all_see, host_port, raw, sh, wait_for
from .harness import main, scenario


@scenario("isolate_one")  # strict: the single-node claim is the one the design makes
async def isolate_one(h) -> dict:
    victim = h.rng.choice(h.c.live)
    rest = h.c.others(victim)
    h.w.mark(f"ISOLATE {victim}")
    await sh("split", victim, " ".join(rest))
    detect = await wait_for(all_see(rest, victim, "dead"), 60, f"{victim} DEAD to majority")
    await wait_for(all_see([victim], rest, "dead"), 60, "victim to see everyone DEAD")
    await asyncio.sleep(8)  # long enough for stale reads / failed coordinator attempts to show
    h.w.mark("HEAL")
    await sh("heal")
    t = time.monotonic()
    await h.c.wait_alive(120)
    return {
        "victim": victim,
        "detect_s": round(detect, 1),
        "heal_to_alive_s": round(time.monotonic() - t, 1),
    }


async def divergence(h, keys: list[str]) -> tuple[int, Counter]:
    """How many keys have replicas that disagree (LOCALGET on each owner), and the
    majority value per key."""
    node = h.c.live[0]

    async def one(k: str):
        owners = await raw(host_port(node), "WHERE", k)
        if not isinstance(owners, list):
            return []
        return await asyncio.gather(*(raw(host_port(o.decode()), "LOCALGET", k) for o in owners))

    divergent, finals = 0, Counter()
    for i in range(0, len(keys), 50):
        for vals in await asyncio.gather(*(one(k) for k in keys[i : i + 50])):
            if not vals:
                continue
            if len(set(vals)) > 1:
                divergent += 1
            finals[max(set(vals), key=vals.count)] += 1
    return divergent, finals


@scenario("split_3_2", strict=False)  # informational: this scenario measures, it doesn't promise
async def split_3_2(h) -> dict:
    side_a, side_b = h.c.live[:3], h.c.live[3:]
    keys = [f"conflict:{i}" for i in range(200)]
    for k in keys:
        await h.w.client.set(k, "base")
    h.w.mark(f"SPLIT {side_a} | {side_b}")
    await sh("split", " ".join(side_a), " ".join(side_b))
    await wait_for(all_see(side_a, side_b, "dead"), 60, "side A to see B DEAD")
    await wait_for(all_see(side_b, side_a, "dead"), 60, "side B to see A DEAD")

    acked_a = acked_b = 0
    for k in keys:  # B's write is strictly later in real time (same host clock)
        acked_a += await raw(host_port(side_a[0]), "SET", k, "from-A", timeout=5) == "OK"
        await asyncio.sleep(0.01)
        acked_b += await raw(host_port(side_b[0]), "SET", k, "from-B", timeout=5) == "OK"
    await asyncio.sleep(3)
    h.w.mark("HEAL")
    await sh("heal")
    await h.c.settle(120)

    d_heal, finals = await divergence(h, keys)
    for n in h.c.live:  # a read sweep through every node: read repair only
        for i in range(0, len(keys), 50):
            await asyncio.gather(*(raw(host_port(n), "GET", k) for k in keys[i : i + 50]))
    await asyncio.sleep(2)
    d_reads, _ = await divergence(h, keys)
    for n in h.c.live:  # explicit catch-up pull on every node
        await raw(host_port(n), "CLUSTER", "SYNC", timeout=60)
    d_sync, finals_after = await divergence(h, keys)
    both = min(acked_a, acked_b)
    return {
        "acked_on_side_A": acked_a,
        "acked_on_side_B": acked_b,
        "divergent_after_heal": d_heal,
        "after_read_sweep": d_reads,
        "after_cluster_sync": d_sync,
        "final_values": {k.decode() if k else "nil": v for k, v in finals_after.items()},
        "acked_writes_silently_lost": both
        if both and finals_after.get(b"from-B")
        else "see final_values",
    }


if __name__ == "__main__":
    main(["isolate_one", "split_3_2"])
