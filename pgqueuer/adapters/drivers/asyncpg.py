"""AsyncPG driver implementations."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import TYPE_CHECKING

from typing_extensions import Self

from pgqueuer.core.tm import TaskManager
from pgqueuer.ports.driver import Driver, NotifyCallback

if TYPE_CHECKING:
    import asyncpg


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
        connection: asyncpg.Connection,
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

    @property
    def tm(self) -> TaskManager:
        return TaskManager()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None: ...


class AsyncpgPoolDriver(Driver):
    """
    Implements the Driver protocol using AsyncPGPool for PostgreSQL database operations.

    This class manages asynchronous database operations using a connection pool.
    It ensures thread safety through query locking and supports PostgreSQL LISTEN/NOTIFY
    functionality with dedicated listeners. The driver does not close the provided
    pool; callers are responsible for managing the pool lifecycle by closing it
    manually or using the pool as a context manager.
    """

    def __init__(
        self,
        pool: asyncpg.Pool,
    ) -> None:
        self._shutdown = asyncio.Event()
        self._pool = pool
        self._listener_connection: asyncpg.pool.PoolConnectionProxy | None = None
        self._lock = asyncio.Lock()
        self._entered = 0

    async def fetch(
        self,
        query: str,
        *args: object,
    ) -> list[dict[str, object]]:
        return [dict(x) for x in await self._pool.fetch(query, *args)]

    async def execute(
        self,
        query: str,
        *args: object,
    ) -> str:
        return await self._pool.execute(query, *args)

    async def notify(self, channel: str, payload: str) -> None:
        await self._pool.execute("SELECT pg_notify($1, $2)", channel, payload)

    async def add_listener(
        self,
        channel: str,
        callback: NotifyCallback,
    ) -> None:
        async with self._lock:
            if self._listener_connection is None:
                # The listener keeps this connection; queries need at least one more.
                if self._pool.get_max_size() < 2:
                    raise RuntimeError(
                        "Pool max size must be greater than 2 to ensure connections are available."
                    )
                self._listener_connection = await self._pool.acquire()

            await self._listener_connection.add_listener(channel, Relay(callback))

    async def remove_listener(
        self,
        channel: str,
        callback: NotifyCallback,
    ) -> None:
        async with self._lock:
            if self._listener_connection is not None:
                await self._listener_connection.remove_listener(channel, Relay(callback))

    @property
    def shutdown(self) -> asyncio.Event:
        return self._shutdown

    @property
    def tm(self) -> TaskManager:
        return TaskManager()

    async def __aenter__(self) -> Self:
        self._entered += 1
        return self

    async def __aexit__(self, *_: object) -> None:
        # PgQueuer.run enters one driver from two managers; only the last exit may release.
        self._entered -= 1
        if self._entered > 0:
            return
        async with self._lock:
            if self._listener_connection is not None:
                await self._listener_connection.reset()
                await self._pool.release(self._listener_connection)
                self._listener_connection = None
