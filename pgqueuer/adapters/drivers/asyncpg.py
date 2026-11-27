"""AsyncPG driver implementations."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, NamedTuple, Self

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


def _released(connection: asyncpg.pool.PoolConnectionProxy) -> bool:
    """True if the connection has closed or asyncpg has already returned it to the pool."""
    import asyncpg

    try:
        return connection.is_closed()
    except asyncpg.InterfaceError:
        return True


class AsyncpgDriver(Driver):
    """AsyncPG implementation of the `Driver` protocol.

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

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None: ...


class Subscription(NamedTuple):
    channel: str
    relay: Relay


async def return_to_pool(
    pool: asyncpg.Pool,
    connection: asyncpg.pool.PoolConnectionProxy,
) -> None:
    """Reset the connection and give it back to the pool."""
    import asyncpg

    # asyncpg returns a connection that dies to the pool by itself; reset() would raise.
    if not _released(connection):
        try:
            await connection.reset()
        except (asyncpg.PostgresConnectionError, asyncpg.InterfaceError, OSError):
            logging.getLogger("pgqueuer").warning(
                "Listener connection lost during reset", exc_info=True
            )
    await pool.release(connection)


@dataclass
class PoolListener:
    """Hold one pool connection for LISTEN while any callback is subscribed."""

    pool: asyncpg.Pool
    connection: asyncpg.pool.PoolConnectionProxy | None = field(init=False, default=None)
    subscriptions: set[Subscription] = field(init=False, default_factory=set)
    lock: asyncio.Lock = field(init=False, default_factory=asyncio.Lock)

    async def add(self, channel: str, callback: NotifyCallback) -> None:
        async with self.lock:
            if self.connection is not None and _released(self.connection):
                # The LISTENs ended with the lost connection. Start again on a new one.
                lost, self.connection = self.connection, None
                self.subscriptions.clear()
                await self.pool.release(lost)
            if self.connection is None:
                # The listener keeps this connection; queries need at least one more.
                if self.pool.get_max_size() < 2:
                    raise RuntimeError(
                        "Pool max size must be greater than 2 to ensure connections are available."
                    )
                self.connection = await self.pool.acquire()
            subscription = Subscription(channel, Relay(callback))
            await self.connection.add_listener(channel, subscription.relay)
            self.subscriptions.add(subscription)

    async def remove(self, channel: str, callback: NotifyCallback) -> None:
        async with self.lock:
            subscription = Subscription(channel, Relay(callback))
            if self.connection is None or subscription not in self.subscriptions:
                return
            # A lost connection has no listener to remove.
            if not _released(self.connection):
                await self.connection.remove_listener(channel, subscription.relay)
            self.subscriptions.discard(subscription)
            if self.subscriptions:
                return
            connection, self.connection = self.connection, None
        await return_to_pool(self.pool, connection)

    async def close(self) -> None:
        async with self.lock:
            connection, self.connection = self.connection, None
            self.subscriptions.clear()
        if connection is not None:
            await return_to_pool(self.pool, connection)


class AsyncpgPoolDriver(Driver):
    """Implements the Driver protocol using AsyncPGPool for PostgreSQL database operations.

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
        self.listener = PoolListener(pool)
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
        await self.listener.add(channel, callback)

    async def remove_listener(
        self,
        channel: str,
        callback: NotifyCallback,
    ) -> None:
        await self.listener.remove(channel, callback)

    @property
    def shutdown(self) -> asyncio.Event:
        return self._shutdown

    async def __aenter__(self) -> Self:
        self._entered += 1
        return self

    async def __aexit__(self, *_: object) -> None:
        # PgQueuer.run enters one driver from two managers; only the last exit may release.
        self._entered -= 1
        if self._entered > 0:
            return
        await self.listener.close()
