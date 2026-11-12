#!/usr/bin/env bash
# Clone the pushed repo into a temp dir and run the README's commands, nothing else.
set -euo pipefail
REPO="${1:?usage: dryrun.sh <git url or local path>}"
T="$(mktemp -d)"; trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$T"' EXIT
git clone --depth 1 "$REPO" "$T/repo" && cd "$T/repo"
python3.12 -m venv .venv && source .venv/bin/activate
pip install -q -e ".[dev]"
ruff check . && python tools/check_claims.py && pytest -q
./scripts/run_cluster.sh 3 >/dev/null 2>&1 &
sleep 12
python - <<'EOF'
import asyncio
from client.cluster import ClusterClient

async def main():
    async with ClusterClient([("127.0.0.1", 7001)]) as cl:
        assert len(cl.nodes()) == 3, cl.nodes()
        assert await cl.set("hello", "world", ex=30)
        assert await cl.get("hello") == b"world"
        print("smoke OK:", cl.nodes())

asyncio.run(main())
EOF
echo "DRY RUN PASSED"