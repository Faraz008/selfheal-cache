import asyncio

import pytest

from node.rpc import RpcClient, RpcError, RpcServer

from .conftest import free_port


async def echo(a):
    return a


async def slow(a):
    await asyncio.sleep(a["s"])
    return "done"


async def boom(a):
    raise ValueError("bad")


HANDLERS = {"echo": echo, "slow": slow, "boom": boom}


@pytest.fixture
async def rpc():
    srv = RpcServer("127.0.0.1", 0, HANDLERS)
    await srv.start()
    cli = RpcClient("127.0.0.1", srv.port, timeout_s=0.3)
    yield srv, cli
    await cli.close()
    await srv.stop()


async def test_echo_roundtrip(rpc):
    _, cli = rpc
    assert await cli.call("echo", {"x": [1, "two", None]}) == {"x": [1, "two", None]}


async def test_remote_exception_becomes_rpc_error(rpc):
    _, cli = rpc
    with pytest.raises(RpcError, match="bad"):
        await cli.call("boom")


async def test_unknown_op(rpc):
    _, cli = rpc
    with pytest.raises(RpcError, match="unknown op"):
        await cli.call("nope")


async def test_timeout_then_connection_still_usable(rpc):
    _, cli = rpc
    with pytest.raises(RpcError, match="timed out"):
        await cli.call("slow", {"s": 1})
    assert await cli.call("echo", {"ok": 1}) == {"ok": 1}  # late reply was ignored


async def test_many_concurrent_calls_are_matched_to_callers(rpc):
    _, cli = rpc
    results = await asyncio.gather(*(cli.call("echo", {"i": i}) for i in range(200)))
    assert [r["i"] for r in results] == list(range(200))


async def test_slow_call_does_not_block_fast_call(rpc):
    _, cli = rpc
    slow_task = asyncio.create_task(cli.call("slow", {"s": 0.2}, timeout=2))
    t0 = asyncio.get_running_loop().time()
    await cli.call("echo", {})
    assert asyncio.get_running_loop().time() - t0 < 0.1
    assert await slow_task == "done"


async def test_connection_refused_is_rpc_error():
    cli = RpcClient("127.0.0.1", free_port(), timeout_s=0.3)
    with pytest.raises(RpcError):
        await cli.call("echo")
    await cli.close()


async def test_reconnects_after_server_restart():
    port = free_port()
    srv = RpcServer("127.0.0.1", port, HANDLERS)
    await srv.start()
    cli = RpcClient("127.0.0.1", port, timeout_s=0.3)
    assert await cli.call("echo", {"a": 1}) == {"a": 1}
    await srv.stop()
    with pytest.raises(RpcError):
        await cli.call("echo")
    srv2 = RpcServer("127.0.0.1", port, HANDLERS)
    await srv2.start()
    assert await cli.call("echo", {"a": 2}) == {"a": 2}
    await cli.close()
    await srv2.stop()


async def test_wedged_connection_is_reset_after_repeated_timeouts(rpc):
    _, cli = rpc
    for _ in range(4):  # 0.3 s timeout each: the 4th is >0.9 s after the last frame
        with pytest.raises(RpcError):
            await cli.call("slow", {"s": 5})
    assert await cli.call("echo", {"ok": 1}) == {"ok": 1}
    assert cli.connects >= 2  # it dialed a fresh connection
