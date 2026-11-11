import asyncio
import time
from collections import Counter

from .common import all_see, host_port, info, raw, sh, wait_for
from .harness import main, scenario

N_KEYS = 100


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


async def divergence(h, keys: list[str]) -> tuple[int, dict[str, str | None]]:
    """(# keys whose replicas disagree via LOCALGET, majority value per key)."""
    node = h.c.live[0]

    async def one(k: str):
        owners = await raw(host_port(node), "WHERE", k)
        if not isinstance(owners, list):
            return k, []
        return k, await asyncio.gather(*(raw(host_port(o.decode()), "LOCALGET", k) for o in owners))

    divergent, final = 0, {}
    for i in range(0, len(keys), 50):
        for k, vals in await asyncio.gather(*(one(k) for k in keys[i : i + 50])):
            if not vals:
                continue
            if len({repr(v) for v in vals}) > 1:
                divergent += 1
            top = max(vals, key=vals.count)
            final[k] = top.decode() if isinstance(top, bytes) else None
    return divergent, final


@scenario("split_3_2", strict=False)  # informational: measures, doesn't promise
async def split_3_2(h) -> dict:
    side_a, side_b = h.c.live[:3], h.c.live[3:]
    keys = [f"conflict:{i}" for i in range(N_KEYS)]
    for k in keys:
        await h.w.client.set(k, "base")
    h.w.mark(f"SPLIT {side_a} | {side_b}")
    await sh("split", " ".join(side_a), " ".join(side_b))
    await wait_for(all_see(side_a, side_b, "dead"), 60, "side A to see B DEAD")
    await wait_for(all_see(side_b, side_a, "dead"), 60, "side B to see A DEAD")

    acked: dict[str, list[str]] = {k: [] for k in keys}
    for k in keys:  # B's write is strictly later in real time (one host clock)
        if await raw(host_port(side_a[0]), "SET", k, "from-A", timeout=5) == "OK":
            acked[k].append("from-A")
        await asyncio.sleep(0.01)
        if await raw(host_port(side_b[0]), "SET", k, "from-B", timeout=5) == "OK":
            acked[k].append("from-B")

    split_brain = unavailable = 0  # what each side serves while the partition lasts
    for k in keys:
        ra = await raw(host_port(side_a[0]), "GET", k, timeout=5)
        rb = await raw(host_port(side_b[0]), "GET", k, timeout=5)
        unavailable += sum(not isinstance(r, bytes) for r in (ra, rb))
        split_brain += isinstance(ra, bytes) and isinstance(rb, bytes) and ra != rb

    h.w.mark("HEAL")
    await sh("heal")
    await h.c.settle(120)
    d_heal, _ = await divergence(h, keys)
    for n in h.c.live:  # a read sweep through every node: read repair only
        for i in range(0, len(keys), 50):
            await asyncio.gather(*(raw(host_port(n), "GET", k) for k in keys[i : i + 50]))
    await asyncio.sleep(2)
    d_reads, _ = await divergence(h, keys)
    for n in h.c.live:  # explicit catch-up pull on every node
        await raw(host_port(n), "CLUSTER", "SYNC", timeout=60)
    d_sync, final = await divergence(h, keys)

    # A value that won although its write was reported as FAILED.
    unacked_won = sum(
        1 for k in keys if final.get(k) in ("from-A", "from-B") and final[k] not in acked[k]
    )
    return {
        "mode": (await info(side_a[0]) or {}).get("consistency"),
        "acked_by_A": sum("from-A" in v for v in acked.values()),
        "acked_by_B": sum("from-B" in v for v in acked.values()),
        "keys_acked_on_both_sides": sum(len(v) == 2 for v in acked.values()),
        "split_brain_keys_during_partition": split_brain,
        "unavailable_reads_during_partition": unavailable,
        "divergent_after_heal": d_heal,
        "after_read_sweep": d_reads,
        "after_cluster_sync": d_sync,
        "unacked_writes_that_won": unacked_won,
    }


if __name__ == "__main__":
    main(["isolate_one", "split_3_2"])
