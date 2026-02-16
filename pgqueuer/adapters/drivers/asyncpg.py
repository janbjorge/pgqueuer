"""AsyncPG driver implementations."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Callable

from typing_extensions import Self

from pgqueuer.core.tm import TaskManager
from pgqueuer.ports.driver import Driver

if TYPE_CHECKING:
    import asyncpg


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
        """Initialize the driver with an AsyncPG connection."""
        self._shutdown = asyncio.Event()
        self._connection = connection
        self._lock = asyncio.Lock()

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        async with self._lock:
            return [dict(x) for x in await self._connection.fetch(query, *args)]

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        async with self._lock:
            return await self._connection.execute(query, *args)

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        async with self._lock:
            await self._connection.add_listener(
                channel,
                lambda *x: callback(x[-1]),
            )

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
        """
        Initialize the AsyncpgPoolDriver with a connection pool.
        """
        self._shutdown = asyncio.Event()
        self._pool = pool
        self._listener_connection: asyncpg.pool.PoolConnectionProxy | None = None
        self._lock = asyncio.Lock()
        if self._pool.get_max_size() < 2:
            raise RuntimeError(
                "Pool max size must be greater than 2 to ensure connections are available."
            )

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        return [dict(x) for x in await self._pool.fetch(query, *args)]

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        return await self._pool.execute(query, *args)

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        """Add a database listener with locking to manage concurrency."""
        async with self._lock:
            if self._listener_connection is None:
                self._listener_connection = await self._pool.acquire()

            await self._listener_connection.add_listener(
                channel,
                lambda *x: callback(x[-1]),
            )

    @property
    def shutdown(self) -> asyncio.Event:
        return self._shutdown

    @property
    def tm(self) -> TaskManager:
        return TaskManager()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        async with self._lock:
            if self._listener_connection is not None:
                await self._listener_connection.reset()
                await self._pool.release(self._listener_connection)
                self._listener_connection = None
