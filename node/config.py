import argparse
import json
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Member:
    node_id: str
    host: str
    port: int  # client-facing RESP port
    rpc_port: int  # internal node-to-node port

    @classmethod
    def parse(cls, spec: str) -> "Member":
        """'n1@127.0.0.1:7001:8001' -> Member(...)"""
        try:
            node_id, addr = spec.split("@", 1)
            host, port, rpc_port = addr.rsplit(":", 2)
            return cls(node_id, host, int(port), int(rpc_port))
        except ValueError:
            raise SystemExit(f"bad member {spec!r}; expected id@host:port:rpc_port") from None


@dataclass
class NodeConfig:
    node_id: str = "n1"
    host: str = "127.0.0.1"
    port: int = 7001
    rpc_port: int = 8001
    members: list[str] = field(default_factory=list)  # "id@host:port:rpc", INCLUDING self
    replicas: int = 3  # N
    write_acks: int = 1  # W
    rpc_timeout_ms: int = 500
    tombstone_grace_ms: int = 600_000
    vnodes: int = 150
    sweep_interval_ms: int = 100
    log_level: str = "INFO"

    def member_list(self) -> list[Member]:
        if not self.members:  # single-node mode
            return [Member(self.node_id, self.host, self.port, self.rpc_port)]
        return [Member.parse(s) for s in self.members]

    def validate(self) -> None:
        ids = [m.node_id for m in self.member_list()]
        if len(set(ids)) != len(ids):
            raise SystemExit("duplicate node ids in members")
        if self.node_id not in ids:
            raise SystemExit(f"{self.node_id} is not listed in members")
        if self.replicas < 1:
            raise SystemExit("replicas must be >= 1")
        if not 1 <= self.write_acks <= self.replicas:
            raise SystemExit("write_acks must be between 1 and replicas")

    @classmethod
    def load(cls, argv: list[str] | None = None) -> "NodeConfig":
        """Precedence: defaults < JSON file < CLI flags."""
        p = argparse.ArgumentParser(prog="python -m node")
        p.add_argument("--config")
        p.add_argument("--node-id")
        p.add_argument("--host")
        p.add_argument("--port", type=int)
        p.add_argument("--rpc-port", type=int)
        p.add_argument("--members", help="comma-separated id@host:port:rpc_port")
        p.add_argument("--replicas", type=int)
        p.add_argument("--write-acks", type=int)
        p.add_argument("--log-level")
        a = p.parse_args(argv)

        cfg = cls()
        if a.config:
            for k, v in json.loads(Path(a.config).read_text()).items():
                if not hasattr(cfg, k):
                    raise SystemExit(f"unknown config key: {k}")
                setattr(cfg, k, v)
        for name in ("node_id", "host", "port", "rpc_port", "replicas", "write_acks", "log_level"):
            v = getattr(a, name)
            if v is not None:
                setattr(cfg, name, v)
        if a.members is not None:
            cfg.members = [x for x in a.members.split(",") if x]
        return cfg
