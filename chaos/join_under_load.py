import argparse
import asyncio
import statistics
import subprocess
import sys
import time
from pathlib import Path

from client.cluster import ClusterClient, ClusterUnavailable, read_reply
from node.resp import encode_command


def spawn(i: int) -> subprocess.Popen:
    args = [
        sys.executable,
        "-m",
        "node",
        "--node-id",
        f"n{i}",
        "--port",
        str(7200 + i),
        "--rpc-port",
        str(7800 + i),
        "--replicas",
        "3",
        "--write-acks",
        "2",
    ]
    if i > 1:
        args += ["--seeds", "127.0.0.1:7801"]
    return subprocess.Popen(args, stderr=open(f"logs/join-n{i}.log", "ab"))


def log(t0: float, msg: str) -> None:
    print(f"[{time.monotonic() - t0:5.1f}s] {msg}", flush=True)


async def raw(port: int, *parts):
    r, w = await asyncio.open_connection("127.0.0.1", port)
    w.write(encode_command(*parts))
    reply = await read_reply(r)
    w.close()
    return reply


async def wait_state(client, node: str, state: str, timeout: float) -> float:
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        text = (await client.execute("CLUSTER", "NODES")).decode()
        if any(f[0] == node and f[2] == state for f in (ln.split() for ln in text.splitlines())):
            return time.monotonic()
        await asyncio.sleep(0.25)
    raise SystemExit(f"{node} never reached state {state}")


async def traffic(client, stop, res, wid):
    i = 0
    while not stop.is_set():
        k, v = f"j{wid}:{i}", f"v{i}"
        i += 1
        t = time.monotonic()
        try:
            await client.set(k, v)
            res["acked"][k] = v
        except ClusterUnavailable:
            res["failed"] += 1
        res["lat"].append(time.monotonic() - t)
        await asyncio.sleep(0.002)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--preload", type=int, default=20000)
    ap.add_argument("--workers", type=int, default=16)
    a = ap.parse_args()
    Path("logs").mkdir(exist_ok=True)
    procs = {i: spawn(i) for i in (1, 2, 3)}
    t0 = time.monotonic()
    res = {"acked": {}, "failed": 0, "lat": []}
    client = ClusterClient([("127.0.0.1", 7201)], retries=1, request_timeout=3.0)
    try:
        await asyncio.sleep(1)
        for _ in range(60):
            if await client.refresh() and len(client.nodes()) == 3:
                break
            await asyncio.sleep(0.5)
        else:
            sys.exit("cluster did not converge")
        await client.start()
        for i in range(0, a.preload, 200):
            await asyncio.gather(
                *(client.set(f"pre:{j}", f"v{j}") for j in range(i, min(i + 200, a.preload)))
            )
            res["acked"].update({f"pre:{j}": f"v{j}" for j in range(i, min(i + 200, a.preload))})
        log(t0, f"preloaded {a.preload} keys")

        stop = asyncio.Event()
        workers = [asyncio.create_task(traffic(client, stop, res, w)) for w in range(a.workers)]
        await asyncio.sleep(5)

        procs[4] = spawn(4)
        t_join = time.monotonic()
        log(t0, "STARTED n4 (joining)")
        t_alive = await wait_state(client, "n4", "alive", 60)
        log(t0, f"n4 ALIVE after {t_alive - t_join:.1f}s")
        print((await raw(7204, "INFO")).decode())
        await asyncio.sleep(5)

        log(t0, "n2: CLUSTER LEAVE")
        t_leave = time.monotonic()
        print("reply:", await raw(7202, "CLUSTER", "LEAVE"))
        log(t0, f"n2 handed off and left in {time.monotonic() - t_leave:.1f}s")
        await wait_state(client, "n2", "left", 30)
        await asyncio.sleep(5)
        stop.set()
        await asyncio.gather(*workers)

        await asyncio.sleep(3)  # let cleanup and hint replay settle
        keys = list(res["acked"])
        lost = 0
        for i in range(0, len(keys), 200):
            chunk = keys[i : i + 200]
            got = await asyncio.gather(*(client.get(k) for k in chunk))
            lost += sum(g != res["acked"][k].encode() for k, g in zip(chunk, got, strict=True))
        lat = sorted(res["lat"])
        print("\n=== RESULT ===")
        print(
            f"acked_writes={len(keys)} "
            f"failed_requests={res['failed']} "
            f"retries={client.stats['retries']}"
        )
        print(f"lost_acked_writes={lost}")
        print(
            f"latency p50={statistics.median(lat) * 1000:.1f}ms "
            f"p99={lat[int(len(lat) * 0.99)] * 1000:.1f}ms max={lat[-1] * 1000:.0f}ms"
        )
        print("PASS" if res["failed"] == 0 and lost == 0 else "FAIL")
    finally:
        await client.close()
        for p in procs.values():
            p.kill()


asyncio.run(main())
