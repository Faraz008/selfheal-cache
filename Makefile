.PHONY: test lint up down chaos demo
test:  ; pytest -q
lint:  ; ruff check . && ruff format --check . && python tools/check_claims.py
up:    ; ./scripts/cluster.sh up
down:  ; ./scripts/cluster.sh down
chaos: ; python -m chaos.harness