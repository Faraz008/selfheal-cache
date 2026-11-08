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

            if not node_id or not host:
                raise ValueError

            return cls(node_id, host, int(port), int(rpc_port))

        except ValueError:
            raise SystemExit(f"bad member {spec!r}; expected id@host:port:rpc_port") from None


@dataclass
class NodeConfig:
    node_id: str = "n1"
    host: str = "127.0.0.1"
    port: int = 7001
    rpc_port: int = 8001

    members: list[str] = field(default_factory=list)
    # "id@host:port:rpc", INCLUDING self

    advertise_host: str = ""
    # Address peers use to reach this node.
    # Defaults to host when empty.

    seeds: list[str] = field(default_factory=list)
    # "host:rpc_port"

    replicas: int = 3  # N
    write_acks: int = 1  # W

    rpc_timeout_ms: int = 500
    tombstone_grace_ms: int = 600_000
    vnodes: int = 150
    sweep_interval_ms: int = 100

    gossip_interval_ms: int = 1000
    gossip_fanout: int = 2

    probe_interval_ms: int = 1000
    probe_timeout_ms: int = 500
    indirect_probes: int = 2
    suspect_timeout_ms: int = 5000

    # Read coordination
    hedge_ms: int = 50
    read_fanout: int = 2  # replicas consulted per read; newest version wins

    # Hinted handoff
    hint_ttl_ms: int = 300_000
    max_hints: int = 100_000
    hint_batch: int = 200
    hint_replay_interval_ms: int = 1000

    # Rebalancing and data transfer
    rebalance_settle_ms: int = 3000
    # Wait after join/leave announcement before moving data.

    transfer_batch: int = 500
    # Records per stream page.

    transfer_rate: int = 5000
    # Records/sec cap for any transfer; 0 = unlimited.

    join_max_wait_ms: int = 60_000
    # Give up waiting for sources after this and go ALIVE anyway.

    sync_on_start: bool = True
    # Static-member nodes catch up from peers at boot.

    # Cleanup
    cleanup_interval_ms: int = 5000
    cleanup_delay_ms: int = 10_000
    # Quiet period after a ring change before cleanup runs.

    cleanup_batch: int = 200

    # Graceful leave and sessions
    leave_timeout_ms: int = 120_000
    session_ttl_ms: int = 60_000

    log_level: str = "INFO"

    def member_list(self) -> list[Member]:
        if not self.members:
            # Single-node mode
            return [
                Member(
                    self.node_id,
                    self.advertise_host or self.host,
                    self.port,
                    self.rpc_port,
                )
            ]

        return [Member.parse(s) for s in self.members]

    def seed_addrs(self) -> list[tuple[str, int]]:
        """Parse seed addresses into (host, rpc_port) tuples."""
        out = []

        for s in self.seeds:
            try:
                host, port = s.rsplit(":", 1)

                if not host:
                    raise ValueError

                out.append((host, int(port)))

            except ValueError:
                raise SystemExit(f"bad seed {s!r}; expected host:rpc_port") from None

        return out

    def validate(self) -> None:
        members = self.member_list()
        ids = [m.node_id for m in members]

        if len(set(ids)) != len(ids):
            raise SystemExit("duplicate node ids in members")

        if self.node_id not in ids:
            raise SystemExit(f"{self.node_id} is not listed in members")

        if self.replicas < 1:
            raise SystemExit("replicas must be >= 1")

        if not 1 <= self.write_acks <= self.replicas:
            raise SystemExit("write_acks must be between 1 and replicas")

        # Validate member endpoints.
        for member in members:
            if (
                not member.host
                or not 0 <= member.port <= 65535
                or not 0 <= member.rpc_port <= 65535
            ):
                raise SystemExit(f"invalid endpoint for member {member.node_id}")

        # Validate seeds.
        self.seed_addrs()

        # Validate timing and probing configuration.
        positive_values = {
            "gossip_interval_ms": self.gossip_interval_ms,
            "probe_interval_ms": self.probe_interval_ms,
            "probe_timeout_ms": self.probe_timeout_ms,
            "suspect_timeout_ms": self.suspect_timeout_ms,
            "rpc_timeout_ms": self.rpc_timeout_ms,
        }

        for name, value in positive_values.items():
            if value <= 0:
                raise SystemExit(f"{name} must be > 0")

        if self.gossip_fanout < 1:
            raise SystemExit("gossip_fanout must be >= 1")

        if self.indirect_probes < 0:
            raise SystemExit("indirect_probes must be >= 0")

        # Validate read coordination and hinted handoff.
        if self.read_fanout < 1:
            raise SystemExit("read_fanout must be >= 1")

        if self.hint_ttl_ms > self.tombstone_grace_ms:
            raise SystemExit("hint_ttl_ms must be <= tombstone_grace_ms (resurrection risk)")

        # Validate data transfer and cleanup configuration.
        if self.transfer_batch < 1 or self.cleanup_batch < 1:
            raise SystemExit("transfer_batch and cleanup_batch must be >= 1")

    @classmethod
    def load(cls, argv: list[str] | None = None) -> "NodeConfig":
        """Precedence: defaults < JSON file < CLI flags."""

        p = argparse.ArgumentParser(prog="python -m node")

        p.add_argument("--config")
        p.add_argument("--node-id")
        p.add_argument("--host")
        p.add_argument("--port", type=int)
        p.add_argument("--rpc-port", type=int)

        p.add_argument(
            "--members",
            help="comma-separated id@host:port:rpc_port",
        )

        p.add_argument("--seeds", help="comma-separated host:rpc_port")
        p.add_argument("--advertise-host")

        p.add_argument("--replicas", type=int)
        p.add_argument("--write-acks", type=int)
        p.add_argument("--log-level")

        a = p.parse_args(argv)

        cfg = cls()

        # 1. Load JSON configuration.
        if a.config:
            try:
                data = json.loads(Path(a.config).read_text())
            except (OSError, json.JSONDecodeError) as exc:
                raise SystemExit(f"cannot load config {a.config!r}: {exc}") from None

            if not isinstance(data, dict):
                raise SystemExit("config JSON must contain an object")

            for k, v in data.items():
                if not hasattr(cfg, k):
                    raise SystemExit(f"unknown config key: {k}")

                setattr(cfg, k, v)

        # 2. Apply CLI overrides.
        simple_names = (
            "node_id",
            "host",
            "port",
            "rpc_port",
            "advertise_host",
            "replicas",
            "write_acks",
            "log_level",
        )

        for name in simple_names:
            v = getattr(a, name)

            if v is not None:
                setattr(cfg, name, v)

        if a.members is not None:
            cfg.members = [x for x in a.members.split(",") if x]

        if a.seeds is not None:
            cfg.seeds = [x for x in a.seeds.split(",") if x]

        cfg.validate()

        return cfg
