import argparse
import asyncio
import random
import statistics
import subprocess
import sys
import time
from pathlib import Path

from client.cluster import ClusterClient, ClusterUnavailable

N = 5


def spawn(i: int) -> subprocess.Popen:
    args = [
        sys.executable,
        "-m",
        "node",
        "--node-id",
        f"n{i}",
        "--port",
        str(7100 + i),
        "--rpc-port",
        str(8100 + i),
        "--replicas",
        "3",
        "--write-acks",
        "2",
    ]
    if i > 1:
        args += ["--seeds", "127.0.0.1:8101"]
    return subprocess.Popen(args, stderr=open(f"logs/chaos-n{i}.log", "ab"))


def log(t0: float, msg: str) -> None:
    print(f"[{time.monotonic() - t0:5.1f}s] {msg}", flush=True)


async def traffic(client, stop: asyncio.Event, res: dict, wid: int) -> None:
    i = 0
    while not stop.is_set():
        k, v = f"c{wid}:{i}", f"v{i}"
        i += 1
        t = time.monotonic()
        try:
            await client.set(k, v)
            res["acked"][k] = v
        except ClusterUnavailable:
            res["failed"] += 1
        res["lat"].append(time.monotonic() - t)
        await asyncio.sleep(0.002)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=30)
    ap.add_argument("--workers", type=int, default=16)
    a = ap.parse_args()
    Path("logs").mkdir(exist_ok=True)
    procs = {i: spawn(i) for i in range(1, N + 1)}
    t0 = time.monotonic()
    res = {"acked": {}, "failed": 0, "lat": []}
    client = ClusterClient([("127.0.0.1", 7101)], retries=1, request_timeout=2.0)
    try:
        await asyncio.sleep(1)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if await client.refresh() and len(client.nodes()) == N:
                break
            await asyncio.sleep(0.5)
        else:
            sys.exit("cluster did not converge")
        await client.start()
        log(t0, f"cluster up: {client.nodes()}")

        stop = asyncio.Event()
        workers = [asyncio.create_task(traffic(client, stop, res, w)) for w in range(a.workers)]
        victim = random.choice(range(2, N + 1))  # never the seed (n1)
        await asyncio.sleep(8)
        procs[victim].kill()  # SIGKILL: no goodbye
        procs[victim].wait()
        log(t0, f"KILLED n{victim}")
        await asyncio.sleep(12)
        procs[victim] = spawn(victim)
        log(t0, f"RESTARTED n{victim}")
        await asyncio.sleep(max(0, a.duration - 20))
        stop.set()
        await asyncio.gather(*workers)

        await asyncio.sleep(2)  # let hint replay settle
        keys = list(res["acked"])
        lost = 0
        for i in range(0, len(keys), 200):
            chunk = keys[i : i + 200]
            got = await asyncio.gather(*(client.get(k) for k in chunk))
            lost += sum(g != res["acked"][k].encode() for k, g in zip(chunk, got))
        lat = sorted(res["lat"])
        print("\n=== RESULT ===")
        print(f"requests={client.stats['requests']}  acked_writes={len(keys)}")
        print(f"failed_requests={res['failed']}  client_retries={client.stats['retries']}")
        print(f"lost_acked_writes={lost}")
        print(
            f"latency p50={statistics.median(lat) * 1000:.1f}ms  "
            f"p99={lat[int(len(lat) * 0.99)] * 1000:.1f}ms  max={lat[-1] * 1000:.0f}ms"
        )
        print("PASS" if res["failed"] == 0 and lost == 0 else "FAIL")
    finally:
        await client.close()
        for p in procs.values():
            p.kill()


asyncio.run(main())
