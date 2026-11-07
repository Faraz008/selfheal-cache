import base64

from .store import Record


def b64e(b: bytes) -> str:
    return base64.b64encode(b).decode("ascii")


def b64d(s: str) -> bytes:
    return base64.b64decode(s)


def rec_to_wire(r: Record) -> dict:
    return {
        "v": None if r.value is None else b64e(r.value),
        "ts": r.version[0],
        "n": r.version[1],
        "x": r.expire_at,
    }


def rec_from_wire(d: dict) -> Record:
    return Record(None if d["v"] is None else b64d(d["v"]), (d["ts"], d["n"]), d["x"])
