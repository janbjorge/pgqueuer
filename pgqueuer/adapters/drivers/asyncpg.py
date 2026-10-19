"""AsyncPG driver implementations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING, Self

from pgqueuer.ports.driver import Driver, NotifyCallback

if TYPE_CHECKING:
    import asyncpg
    import asyncpg.pool


@dataclass(frozen=True)
class Relay:
    """asyncpg listener that passes only the payload on to *callback*.

    asyncpg removes listeners by equality, so remove_listener can pass a new Relay(callback).
    """

    callback: NotifyCallback

    def __call__(self, connection: object, pid: object, channel: object, payload: object) -> None:
        if isinstance(payload, (str, bytes, bytearray)):
            self.callback(payload)


class AsyncpgDriver(Driver):
    """
    AsyncPG implementation of the `Driver` protocol.

    This driver uses an AsyncPG connection to perform asynchronous database operations
    such as fetching records, executing queries, and listening for notifications.
    It ensures thread safety using an asyncio.Lock. The driver does not close the
    provided connection; callers should manage the connection lifecycle
    themselves by closing it manually or using the connection as a context
    manager.
    """

    def __init__(
        self,
        connection: asyncpg.Connection | asyncpg.pool.PoolConnectionProxy,
    ) -> None:
        self._shutdown = asyncio.Event()
        self._connection = connection
        self._lock = asyncio.Lock()

    async def fetch(
        self,
        query: str,
        *args: object,
    ) -> list[dict[str, object]]:
        async with self._lock:
            return [dict(x) for x in await self._connection.fetch(query, *args)]

    async def execute(
        self,
        query: str,
        *args: object,
    ) -> str:
        async with self._lock:
            return await self._connection.execute(query, *args)

    async def notify(self, channel: str, payload: str) -> None:
        async with self._lock:
            await self._connection.execute("SELECT pg_notify($1, $2)", channel, payload)

    async def add_listener(
        self,
        channel: str,
        callback: NotifyCallback,
    ) -> None:
        async with self._lock:
            await self._connection.add_listener(channel, Relay(callback))

    async def remove_listener(
        self,
        channel: str,
        callback: NotifyCallback,
    ) -> None:
        async with self._lock:
            await self._connection.remove_listener(channel, Relay(callback))

    @property
    def shutdown(self) -> asyncio.Event:
        return self._shutdown

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None: ...
