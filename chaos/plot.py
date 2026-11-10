import csv
import json
import sys

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

path = sys.argv[1] if len(sys.argv) > 1 else "docs/chaos_run.csv"
rows = list(csv.DictReader(open(path)))
events = json.load(open(path.replace(".csv", ".events.json")))
t = [int(r["t"]) for r in rows]
fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 6), sharex=True)
a1.plot(t, [float(r["p99"]) for r in rows], label="p99 ms")
a1.plot(t, [float(r["p50"]) for r in rows], label="p50 ms")
a1.set_ylabel("latency (ms)")
a1.set_yscale("log")
a1.legend()
a2.bar(t, [int(r["errors"]) for r in rows], color="crimson", label="failed requests/s")
a2.plot(t, [int(r["retries"]) for r in rows], color="orange", label="client retries/s")
a2.set_ylabel("count")
a2.set_xlabel("seconds")
a2.legend()
for x, label in events:
    if label.startswith("=="):
        continue
    for ax in (a1, a2):
        ax.axvline(x, color="gray", ls="--", alpha=0.5)
    a1.text(x, a1.get_ylim()[1], label, rotation=90, va="top", fontsize=7)
a1.set_title("Cluster under chaos")
plt.savefig(path.replace(".csv", ".png"), dpi=150, bbox_inches="tight")
print("wrote", path.replace(".csv", ".png"))
