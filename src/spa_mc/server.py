"""`spa_mc.start_server`: the shape of `asyncio.start_server`, served by one event loop per CPU core."""

from __future__ import annotations

import asyncio
import socket
from collections.abc import Awaitable, Callable
from typing import Any

from spa_mc.engine import Engine

ClientConnected = Callable[[asyncio.StreamReader, asyncio.StreamWriter], Awaitable[None]]


async def start_server(
    client_connected_cb: ClientConnected | str,
    host: str | None = None,
    port: int | None = None,
    *,
    limit: int = 2**16,
    sock: socket.socket | None = None,
    backlog: int = 100,
    start_serving: bool = True,
    **engine_options: Any,
) -> Server:
    """Start a TCP server whose connections are handled on every CPU core.

    Same arguments as `asyncio.start_server` (`client_connected_cb`, `host`, `port`, `limit`, `sock`, `backlog`,
    `start_serving`), plus any `spa_mc.Engine` option (`workers`, `overload`, `request_timeout`, ...).
    `client_connected_cb` must be a module-level `async def`: each core runs it in its own interpreter, so
    it cannot share global variables with the other cores or with the caller.
    """
    engine = Engine(client_connected_cb, stream_limit=limit, **engine_options)
    try:
        await engine.serve_tcp(host, port or 0, backlog, sock=sock, start_serving=start_serving)
    except BaseException:
        await engine.stop()
        raise
    return Server(engine)


class Server(asyncio.AbstractServer):
    """Behaves like the `asyncio.Server` that `asyncio.start_server` returns."""

    def __init__(self, engine: Engine) -> None:
        self.engine = engine
        self._loop = asyncio.get_running_loop()
        self._sockets = (engine.listener,) if engine.listener is not None else ()
        self._closed: asyncio.Task | None = None
        self._serving_forever: asyncio.Future | None = None

    @property
    def sockets(self) -> tuple[socket.socket, ...]:
        return () if self._closed else self._sockets

    def get_loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def is_serving(self) -> bool:
        return self._closed is None and self.engine.is_serving()

    async def start_serving(self) -> None:
        if self._closed:
            raise RuntimeError(f'server {self!r} is closed')
        self.engine.start_serving()

    async def serve_forever(self) -> None:
        if self._serving_forever is not None:
            raise RuntimeError(f'server {self!r} is already being awaited on serve_forever()')
        await self.start_serving()
        self._serving_forever = self._loop.create_future()
        try:
            await self._serving_forever
        except asyncio.CancelledError:
            self.close()
            await self.wait_closed()
            raise
        finally:
            self._serving_forever = None

    def close(self) -> None:
        """Stop listening now; connections already accepted are finished, and `wait_closed()` waits for them."""
        if self._closed:
            return
        self.engine.stop_serving()
        self._closed = self._loop.create_task(self.engine.stop())
        if self._serving_forever is not None and not self._serving_forever.done():
            self._serving_forever.cancel()

    async def wait_closed(self) -> None:
        if self._closed is not None:
            await asyncio.shield(self._closed)

    async def __aenter__(self) -> Server:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self.close()
        await self.wait_closed()

    def __repr__(self) -> str:
        return f'<spa_mc.Server sockets={self.sockets!r} workers={self.engine.workers}>'
