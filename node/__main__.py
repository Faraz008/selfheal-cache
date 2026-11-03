import asyncio
import logging
import signal

from .config import NodeConfig
from .server import Server


async def run(cfg: NodeConfig) -> None:
    server = Server(cfg)
    await server.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # Windows
            pass
    await stop.wait()
    await server.stop()


def main() -> None:
    cfg = NodeConfig.load()
    logging.basicConfig(
        level=cfg.log_level.upper(),
        format=f"%(asctime)s %(levelname)s node={cfg.node_id} %(message)s",
    )
    try:
        asyncio.run(run(cfg))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
