import argparse
import asyncio
import time

from node.resp import encode_command


async def worker(host, port, n, wid, payload):
    r, w = await asyncio.open_connection(host, port)
    for i in range(n):
        key = f"bench:{wid}:{i % 1000}"
        w.write(encode_command("SET", key, payload))
        await r.readline()
        w.write(encode_command("GET", key))
        hdr = await r.readline()
        await r.readexactly(int(hdr[1:]) + 2)
    w.close()


async def main():
    p = argparse.ArgumentParser()
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=7001)
    p.add_argument("--clients", type=int, default=50)
    p.add_argument("--ops", type=int, default=2000, help="SET+GET pairs per client")
    a = p.parse_args()
    payload = "x" * 64
    t0 = time.perf_counter()
    await asyncio.gather(*(worker(a.host, a.port, a.ops, i, payload) for i in range(a.clients)))
    dt = time.perf_counter() - t0
    total = a.clients * a.ops * 2
    print(f"{total:,} ops in {dt:.2f}s = {total / dt:,.0f} ops/sec")


asyncio.run(main())
