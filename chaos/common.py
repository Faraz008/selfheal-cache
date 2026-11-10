import asyncio
import time

from client.cluster import ClusterClient, encode, read_reply

ADDR_MAP = {f"n{i}:7001": f"127.0.0.1:{7000 + i}" for i in range(1, 7)}
SEEDS = [("127.0.0.1", 7000 + i) for i in range(1, 6)]


def host_port(node: str) -> int:
    return 7000 + int(node[1:])


def make_client(**kw) -> ClusterClient:
    return ClusterClient(SEEDS, addr_map=ADDR_MAP, **kw)


async def raw(port: int, *parts, timeout: float = 2.0):
    """One command on a fresh connection. None on any network failure."""
    try:
        async with asyncio.timeout(timeout):
            r, w = await asyncio.open_connection("127.0.0.1", port)
            try:
                w.write(encode(parts))
                await w.drain()
                return await read_reply(r)
            finally:
                w.close()
    except (OSError, TimeoutError, EOFError, ConnectionError):
        return None


async def view(node: str) -> dict[str, str] | None:
    """{node_id: state} as seen by `node`, or None if it can't be reached."""
    r = await raw(host_port(node), "CLUSTER", "NODES")
    if not isinstance(r, bytes):
        return None
    out = {}
    for line in r.decode().splitlines():
        f = line.split()
        if len(f) >= 3:
            out[f[0]] = f[2]
    return out


async def info(node: str) -> dict[str, str] | None:
    r = await raw(host_port(node), "INFO")
    if not isinstance(r, bytes):
        return None
    return dict(ln.split(":", 1) for ln in r.decode().splitlines() if ":" in ln)


async def sh(*args: str) -> str:
    p = await asyncio.create_subprocess_exec(
        "scripts/cluster.sh",
        *args,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.STDOUT,
    )
    out, _ = await p.communicate()
    if p.returncode:
        raise RuntimeError(f"cluster.sh {' '.join(args)} failed:\n{out.decode()}")
    return out.decode()


async def wait_for(cond, timeout: float, what: str, every: float = 0.2) -> float:
    """Poll an async predicate. Returns seconds waited."""
    t0 = time.monotonic()
    while True:
        if await cond():
            return time.monotonic() - t0
        if time.monotonic() - t0 > timeout:
            raise TimeoutError(f"timed out after {timeout:.0f}s waiting for {what}")
        await asyncio.sleep(every)


def all_see(observers: list[str], subjects, state: str):
    """Predicate: every observer reports every subject in `state`."""
    subs = [subjects] if isinstance(subjects, str) else list(subjects)

    async def cond():
        for o in observers:
            v = await view(o)
            if v is None or any(v.get(s) != state for s in subs):
                return False
        return True

    return cond
