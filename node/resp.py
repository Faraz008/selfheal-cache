import asyncio

MAX_BULK = 512 * 1024 * 1024
MAX_ARGS = 1024 * 1024

OK = b"+OK\r\n"
PONG = b"+PONG\r\n"
NIL = b"$-1\r\n"


class ProtocolError(Exception):
    pass


async def read_command(reader: asyncio.StreamReader) -> list[bytes] | None:
    """Return args, [] for a blank line, or None on clean EOF."""
    line = await reader.readline()
    if not line:
        return None
    if not line.endswith(b"\r\n"):
        raise ProtocolError("unterminated line")
    line = line[:-2]
    if not line:
        return []
    if line[:1] != b"*":  # inline command, e.g. from telnet/nc
        return line.split()
    try:
        n = int(line[1:])
    except ValueError:
        raise ProtocolError("invalid multibulk length") from None
    if n > MAX_ARGS:
        raise ProtocolError("invalid multibulk length")
    args = []
    for _ in range(max(n, 0)):
        hdr = await reader.readline()
        if hdr[:1] != b"$" or not hdr.endswith(b"\r\n"):
            raise ProtocolError("expected '$'")
        try:
            size = int(hdr[1:-2])
        except ValueError:
            raise ProtocolError("invalid bulk length") from None
        if size < 0 or size > MAX_BULK:
            raise ProtocolError("invalid bulk length")
        data = await reader.readexactly(size + 2)
        if data[-2:] != b"\r\n":
            raise ProtocolError("bulk not terminated by CRLF")
        args.append(data[:-2])
    return args


def simple(s: str) -> bytes:
    return b"+" + s.encode() + b"\r\n"


def error(msg: str) -> bytes:
    msg = msg.replace("\r", " ").replace("\n", " ")
    return b"-" + msg.encode() + b"\r\n"


def integer(n: int) -> bytes:
    return b":%d\r\n" % n


def bulk(b: bytes | None) -> bytes:
    return NIL if b is None else b"$%d\r\n%s\r\n" % (len(b), b)


def encode_command(*parts) -> bytes:
    out = [b"*%d\r\n" % len(parts)]
    for p in parts:
        if isinstance(p, str):
            p = p.encode()
        elif isinstance(p, int):
            p = str(p).encode()
        out.append(b"$%d\r\n%s\r\n" % (len(p), p))
    return b"".join(out)
