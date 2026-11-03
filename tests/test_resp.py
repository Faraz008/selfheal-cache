import asyncio

import pytest

from node import resp


async def parse(data: bytes):
    r = asyncio.StreamReader()
    r.feed_data(data)
    r.feed_eof()
    return await resp.read_command(r)


async def test_array():
    assert await parse(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n") == [b"GET", b"k"]


async def test_binary_safe_value():
    assert await parse(b"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n") == [
        b"SET",
        b"k",
        b"a\r\nb",
    ]


async def test_inline():
    assert await parse(b"PING\r\n") == [b"PING"]


async def test_eof():
    assert await parse(b"") is None


async def test_bad_frame():
    with pytest.raises(resp.ProtocolError):
        await parse(b"*1\r\nGET\r\n")


def test_encoders():
    assert resp.integer(5) == b":5\r\n"
    assert resp.bulk(b"hi") == b"$2\r\nhi\r\n"
    assert resp.bulk(None) == b"$-1\r\n"
    assert resp.encode_command("GET", "k") == b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n"
