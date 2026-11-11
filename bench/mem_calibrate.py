import tracemalloc

from node.evict import OVERHEAD
from node.store import Record, Store

N = 100_000
for vlen in (10, 100, 1000):
    tracemalloc.start()
    s = Store()
    for i in range(N):
        s.apply(f"key:{i:07d}".encode(), Record(b"x" * vlen, (i + 1, "n1")))
    real = tracemalloc.get_traced_memory()[0]
    tracemalloc.stop()
    print(
        f"value={vlen:5d}B  accounted={s.mem / N:7.0f} B/rec  real={real / N:7.0f} B/rec  "
        f"real/accounted={real / s.mem:4.2f}  suggested OVERHEAD={OVERHEAD + (real - s.mem) / N:.0f}"
    )
    del s
