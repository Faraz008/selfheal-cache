import argparse
import asyncio

from .common import make_client
from .workload import Workload


async def amain() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--duration", type=float, default=30)
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--csv", default="docs/loadgen.csv")
    a = ap.parse_args()
    client = make_client(retries=1, request_timeout=3.0)
    await client.start()
    w = Workload(client, workers=a.workers)
    await w.start()
    await asyncio.sleep(a.duration)
    await w.pause()
    checked, bad = await w.verify()
    await w.stop()
    w.write_files(a.csv)
    print(
        f"\nops={w.ops} failed={w.failed} retries={client.stats['retries']} stale_reads={w.stale}"
    )
    print(f"verified {checked} keys: {len(bad)} wrong")
    for m in bad[:5]:
        print("  ", m)
    await client.close()


asyncio.run(amain())
