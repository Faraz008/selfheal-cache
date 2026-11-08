import time
from collections.abc import Callable
from dataclasses import dataclass, replace
from enum import Enum

from .config import NodeConfig


class State(str, Enum):
    JOINING = "joining"
    ALIVE = "alive"
    SUSPECT = "suspect"
    LEAVING = "leaving"
    DEAD = "dead"
    LEFT = "left"  # terminal: removed from the ring


_RANK = {
    State.JOINING: 0,
    State.ALIVE: 1,
    State.SUSPECT: 2,
    State.LEAVING: 3,
    State.DEAD: 4,
    State.LEFT: 5,
}
_READ_PRIORITY = {
    State.ALIVE: 0,
    State.SUSPECT: 1,
    State.LEAVING: 1,
    State.JOINING: 2,
    State.DEAD: 3,
    State.LEFT: 5,
}


@dataclass(frozen=True, slots=True)
class Entry:
    node_id: str
    host: str
    port: int  # client (RESP) port
    rpc_port: int
    state: State
    incarnation: int

    def to_wire(self) -> dict:
        return {
            "id": self.node_id,
            "h": self.host,
            "p": self.port,
            "r": self.rpc_port,
            "s": self.state.value,
            "i": self.incarnation,
        }

    @classmethod
    def from_wire(cls, d: dict) -> "Entry":
        return cls(d["id"], d["h"], d["p"], d["r"], State(d["s"]), d["i"])


def supersedes(new: Entry, cur: Entry) -> bool:
    if new.incarnation != cur.incarnation:
        return new.incarnation > cur.incarnation
    return _RANK[new.state] > _RANK[cur.state]


Listener = Callable[[list[Entry]], None]


class Membership:
    def __init__(self, me: Entry, clock: Callable[[], float] = time.monotonic):
        self.me = me.node_id
        self._clock = clock  # MONOTONIC: suspicion timers must survive wall-clock jumps
        self._table: dict[str, Entry] = {me.node_id: me}
        self._suspect_since: dict[str, float] = {}  # local only, never gossiped
        self._listeners: list[Listener] = []

    @classmethod
    def from_config(cls, cfg: NodeConfig, now_ms: Callable[[], int]) -> "Membership":
        joining = not cfg.members and bool(cfg.seeds)
        me = Entry(
            cfg.node_id,
            cfg.advertise_host or cfg.host,
            cfg.port,
            cfg.rpc_port,
            State.JOINING if joining else State.ALIVE,
            now_ms(),  # incarnation = boot time, so a restart outranks its old DEAD record
        )
        m = cls(me)
        for mem in cfg.member_list():
            if mem.node_id != cfg.node_id:  # static bootstrap view: alive, incarnation 0
                m._table[mem.node_id] = Entry(
                    mem.node_id, mem.host, mem.port, mem.rpc_port, State.ALIVE, 0
                )
        return m

    # ---- reads ----
    def subscribe(self, fn: Listener) -> None:
        self._listeners.append(fn)

    @property
    def self_entry(self) -> Entry:
        return self._table[self.me]

    def get(self, node_id: str) -> Entry | None:
        return self._table.get(node_id)

    def entries(self) -> list[Entry]:
        return list(self._table.values())

    def others(self) -> list[Entry]:
        return [
            e for e in self._table.values() if e.node_id != self.me and e.state is not State.LEFT
        ]

    def node_ids(self) -> list[str]:
        return sorted(self._table)

    def snapshot(self) -> list[dict]:
        return [e.to_wire() for e in self._table.values()]

    def is_dead(self, node_id: str) -> bool:
        e = self._table.get(node_id)
        return e is not None and e.state is State.DEAD

    def read_priority(self, node_id: str) -> int:
        e = self._table.get(node_id)
        return 4 if e is None else _READ_PRIORITY[e.state]

    # ---- writes ----
    def _notify(self, changed: list[Entry]) -> None:
        if changed:
            for fn in self._listeners:
                fn(changed)

    def _set(self, e: Entry) -> None:
        old = self._table.get(e.node_id)
        self._table[e.node_id] = e
        if e.state is State.SUSPECT:
            # (re)start the timer only on a NEW suspicion, never on repeated gossip of it
            if old is None or old.state is not State.SUSPECT or old.incarnation != e.incarnation:
                self._suspect_since[e.node_id] = self._clock()
        else:
            self._suspect_since.pop(e.node_id, None)

    def merge(self, remote: list[Entry]) -> list[Entry]:
        """Fold in gossip. Returns the entries that changed our table."""
        changed: list[Entry] = []
        for e in remote:
            if e.node_id == self.me:
                if self._refute(e):
                    changed.append(self.self_entry)
                continue  # we are the only authority on ourselves
            cur = self._table.get(e.node_id)
            if cur is None or supersedes(e, cur):
                self._set(e)
                changed.append(e)
        self._notify(changed)
        return changed

    def _refute(self, claim: Entry) -> bool:
        cur = self.self_entry
        if cur.state in (State.LEAVING, State.LEFT):
            return False  # a node that is leaving does not fight the verdict
        if (
            claim.state not in (State.SUSPECT, State.DEAD, State.LEFT)
            or claim.incarnation < cur.incarnation
        ):
            return False
        nxt = State.JOINING if cur.state is State.JOINING else State.ALIVE
        self._set(replace(cur, state=nxt, incarnation=claim.incarnation + 1))
        return True

    def mark_suspect(self, node_id: str) -> bool:
        cur = self._table.get(node_id)
        if node_id == self.me or cur is None or cur.state not in (State.ALIVE, State.JOINING):
            return False
        e = replace(cur, state=State.SUSPECT)
        self._set(e)
        self._notify([e])
        return True

    def expire_suspects(self, timeout_s: float) -> list[str]:
        now = self._clock()
        dead = []
        for nid, since in list(self._suspect_since.items()):
            if now - since >= timeout_s:
                e = replace(self._table[nid], state=State.DEAD)
                self._set(e)
                dead.append(nid)
        self._notify([self._table[n] for n in dead])
        return dead

    def activate(self) -> None:
        """JOINING -> ALIVE. Bumps incarnation so it outranks any stale suspicion."""
        cur = self.self_entry
        if cur.state is State.JOINING:
            e = replace(cur, state=State.ALIVE, incarnation=cur.incarnation + 1)
            self._set(e)
            self._notify([e])

    def state_of(self, node_id: str) -> State | None:
        e = self._table.get(node_id)
        return None if e is None else e.state

    def ring_node_ids(self) -> list[str]:
        """The ring is every node not LEFT. Liveness never changes it."""
        return sorted(n for n, e in self._table.items() if e.state is not State.LEFT)

    def leaving_ids(self) -> list[str]:
        return sorted(n for n, e in self._table.items() if e.state is State.LEAVING)

    def begin_leave(self) -> None:
        cur = self.self_entry
        if cur.state in (State.LEAVING, State.LEFT):
            return
        e = replace(cur, state=State.LEAVING)  # same incarnation; rank 3 outranks ALIVE/SUSPECT
        self._set(e)
        self._notify([e])

    def finish_leave(self) -> None:
        cur = self.self_entry
        if cur.state is State.LEAVING:
            e = replace(cur, state=State.LEFT)
            self._set(e)
            self._notify([e])

    def cancel_leave(self) -> None:
        """Failed leave: go back to ALIVE. A bumped incarnation outranks the LEAVING rumor."""
        cur = self.self_entry
        if cur.state is State.LEAVING:
            e = replace(cur, state=State.ALIVE, incarnation=cur.incarnation + 1)
            self._set(e)
            self._notify([e])

    def forget(self, node_id: str) -> bool:
        """Operator removal of a node that will never return."""
        cur = self._table.get(node_id)
        if cur is None or node_id == self.me or cur.state is State.LEFT:
            return False
        e = replace(cur, state=State.LEFT)
        self._set(e)
        self._notify([e])
        return True
