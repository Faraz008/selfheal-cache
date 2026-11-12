import asyncio
import csv
import json
import random
import time
from dataclasses import dataclass

from client.cluster import ClusterUnavailable, RespError

UNCERTAIN = object()  # a write failed: it may or may not have applied
MARGIN_MS = 2000  # don't judge TTL keys within 2 s of their deadline


def now_ms() -> int:
    return time.time_ns() // 1_000_000


@dataclass
class Mismatch:
    kind: str  # lost | resurrected | stale-read | unavailable
    key: str
    expected: object
    got: object


class Workload:
    """N workers, each owning a disjoint key range, so the expected state of every key is
    known exactly. Model entry: (value, deadline_ms|None) | UNCERTAIN | absent (never written)."""

    MIX = {"set": 35, "ttl": 10, "get": 45, "del": 10}

    def __init__(self, client, workers=16, keyspace=300, think_ms=2.0, seed=1, mix=None):
        self.client, self.nworkers, self.keyspace = client, workers, keyspace
        self.think, self.seed, self.mix = think_ms / 1000, seed, mix or self.MIX
        self.models: list[dict] = [{} for _ in range(workers)]
        self.ops = self.failed = self.stale = 0
        self.samples: list[Mismatch] = []
        self.rows: list[dict] = []
        self.events: list[tuple[int, str]] = []
        self._win: list[float] = []
        self._gate, self._stop = asyncio.Event(), asyncio.Event()
        self._busy = 0
        self._tasks: list[asyncio.Task] = []
        self._ticker_task: asyncio.Task | None = None
        self.t0 = time.monotonic()

    # ---- lifecycle ----
    async def start(self) -> None:
        self.t0 = time.monotonic()
        self._gate.set()
        self._tasks = [asyncio.create_task(self._worker(i)) for i in range(self.nworkers)]
        self._ticker_task = asyncio.create_task(self._ticker())

    async def stop(self) -> None:
        self._stop.set()
        self._gate.set()
        await asyncio.gather(*self._tasks)
        if self._ticker_task:
            self._ticker_task.cancel()
            await asyncio.gather(self._ticker_task, return_exceptions=True)

    async def pause(self) -> None:
        """Quiesce all workers (needed before verify(): models must not change under it)."""
        self._gate.clear()
        await asyncio.sleep(
            0.05
        )  # let workers already woken by the gate start and register as busy
        while self._busy:
            await asyncio.sleep(0.01)

    def resume(self) -> None:
        self._gate.set()

    # ---- workers ----
    async def _worker(self, wid: int) -> None:
        rng = random.Random(self.seed * 1000 + wid)
        names, weights = zip(*self.mix.items(), strict=True)
        model, seq = self.models[wid], 0
        while not self._stop.is_set():
            await self._gate.wait()
            if self._stop.is_set():
                break
            self._busy += 1  # no await between the gate and this line: pause() can't miss us
            try:
                seq += 1
                await self._do(
                    rng.choices(names, weights)[0],
                    f"w{wid}:{rng.randrange(self.keyspace)}",
                    model,
                    rng,
                    f"{wid}.{seq}",
                )
            finally:
                self._busy -= 1
            await asyncio.sleep(self.think)

    async def _do(self, op, key, model, rng, tag) -> None:
        t = time.monotonic()
        try:
            if op == "get":
                self._check(key, model.get(key), await self.client.get(key))
            elif op == "del":
                await self.client.delete(key)
                model[key] = (None, None)
            else:
                value = f"{tag}:{'x' * rng.randrange(0, 200)}"
                if op == "ttl":
                    px = rng.randrange(3000, 10000)
                    await self.client.set(key, value, px=px)
                    model[key] = (value.encode(), now_ms() + px)
                else:
                    await self.client.set(key, value)
                    model[key] = (value.encode(), None)
        except (ClusterUnavailable, RespError):
            self.failed += 1
            if op != "get":
                model[key] = UNCERTAIN
        self.ops += 1
        self._win.append(time.monotonic() - t)

    # ---- oracle ----
    @staticmethod
    def _expect(entry):
        """-> (expected value, known?)"""
        if entry is UNCERTAIN:
            return None, False
        if entry is None:
            return None, True  # never written
        value, deadline = entry
        if deadline is None:
            return value, True
        now = now_ms()
        if now > deadline + MARGIN_MS:
            return None, True
        if now > deadline - MARGIN_MS:
            return None, False
        return value, True

    def _check(self, key, entry, got) -> None:
        want, known = self._expect(entry)
        if known and got != want:
            self.stale += 1  # read-your-write violated: reported, not failed
            if len(self.samples) < 20:
                self.samples.append(Mismatch("stale-read", key, want, got))

    async def verify(self, chunk: int = 100) -> tuple[int, list[Mismatch]]:
        """Compare every key against the model. Call only while paused."""
        items = [(k, e) for m in self.models for k, e in m.items()]
        bad: list[Mismatch] = []
        checked = 0
        for i in range(0, len(items), chunk):
            part = items[i : i + chunk]
            got = await asyncio.gather(
                *(self.client.get(k) for k, _ in part), return_exceptions=True
            )
            for (k, e), g in zip(part, got, strict=True):
                want, known = self._expect(e)
                if not known:
                    continue
                checked += 1
                if isinstance(g, Exception):
                    bad.append(Mismatch("unavailable", k, want, repr(g)))
                elif g != want:
                    bad.append(Mismatch("lost" if want is not None else "resurrected", k, want, g))
        return checked, bad

    # ---- observability ----
    def snap(self) -> tuple[int, int, int, int]:
        return self.ops, self.failed, self.stale, self.client.stats["retries"]

    def delta(self, before) -> dict:
        o, f, s, r = (c - p for c, p in zip(self.snap(), before, strict=True))
        return {"ops": o, "failed": f, "stale": s, "retries": r}

    def mark(self, label: str) -> None:
        t = int(time.monotonic() - self.t0)
        self.events.append((t, label))
        print(f"[{t:4d}s]   >>> {label}", flush=True)

    def p99_between(self, t0: float, t1: float) -> float:
        rows = [r["p99"] for r in self.rows if t0 <= r["t"] <= t1]
        return max(rows, default=0.0)

    async def _ticker(self) -> None:
        prev = (0, 0, 0, 0)
        while True:
            await asyncio.sleep(1)
            lats, self._win = sorted(self._win), []
            cur = self.snap()
            ops, failed, stale, retries = (c - p for c, p in zip(cur, prev, strict=True))
            prev = cur
            n = len(lats)
            p50 = lats[n // 2] * 1000 if n else 0.0
            p99 = lats[min(n - 1, int(n * 0.99))] * 1000 if n else 0.0
            mx = lats[-1] * 1000 if n else 0.0
            t = int(time.monotonic() - self.t0)
            self.rows.append(
                dict(
                    t=t,
                    ops=ops,
                    errors=failed,
                    retries=retries,
                    stale=stale,
                    p50=round(p50, 1),
                    p99=round(p99, 1),
                    max=round(mx, 1),
                )
            )
            print(
                f"[{t:4d}s] ops/s={ops:5d} err={failed} retry={retries} stale={stale} "
                f"p50={p50:6.1f}ms p99={p99:7.1f}ms max={mx:7.1f}ms",
                flush=True,
            )

    def write_files(self, csv_path: str) -> None:
        with open(csv_path, "w", newline="") as f:
            wr = csv.DictWriter(f, fieldnames=list(self.rows[0]))
            wr.writeheader()
            wr.writerows(self.rows)
        with open(csv_path.replace(".csv", ".events.json"), "w") as f:
            json.dump(self.events, f)
