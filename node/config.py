import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class NodeConfig:
    node_id: str = "node1"
    host: str = "127.0.0.1"
    port: int = 7001
    peers: list[str] = field(default_factory=list)  # "host:port", used from Phase 3
    sweep_interval_ms: int = 100
    log_level: str = "INFO"

    @classmethod
    def load(cls, argv: list[str] | None = None) -> "NodeConfig":
        """Precedence: defaults < JSON file < CLI flags."""
        p = argparse.ArgumentParser(prog="python -m node")
        p.add_argument("--config")
        p.add_argument("--node-id")
        p.add_argument("--host")
        p.add_argument("--port", type=int)
        p.add_argument("--peers", help="comma-separated host:port")
        p.add_argument("--log-level")
        a = p.parse_args(argv)

        cfg = cls()
        if a.config:
            for k, v in json.loads(Path(a.config).read_text()).items():
                if not hasattr(cfg, k):
                    raise SystemExit(f"unknown config key: {k}")
                setattr(cfg, k, v)
        if a.node_id:
            cfg.node_id = a.node_id
        if a.host:
            cfg.host = a.host
        if a.port is not None:
            cfg.port = a.port
        if a.peers is not None:
            cfg.peers = [x for x in a.peers.split(",") if x]
        if a.log_level:
            cfg.log_level = a.log_level
        return cfg
