"""Database Abstraction Layer for PgQueuer.

This module provides database driver abstractions and a specific implementation
for AsyncPG to handle database operations asynchronously.
"""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Callable, Protocol

from PgQueuer.logconfig import logger
from PgQueuer.tm import TaskManager

if TYPE_CHECKING:
    import asyncpg
    import psycopg


class Driver(Protocol):
    """
    Defines a protocol for database drivers with essential database operations.
    """

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[Any]:
        """Fetch multiple records from the database."""
        raise NotImplementedError

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        """Execute a single query and return a status message."""
        raise NotImplementedError

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        """Add a listener for a specific PostgreSQL NOTIFY channel."""
        raise NotImplementedError

    async def fetchval(
        self,
        query: str,
        *args: Any,
    ) -> Any:
        """Fetch a single value from the database."""
        raise NotImplementedError


class AsyncpgDriver(Driver):
    """
    Implements the Driver protocol using AsyncPG for PostgreSQL database operations.
    """

    def __init__(
        self,
        connection: asyncpg.Connection,
    ) -> None:
        """Initialize the driver with an AsyncPG connection."""
        self.lock = asyncio.Lock()
        self.connection = connection

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list:
        """Fetch records with query locking to ensure thread safety."""
        async with self.lock:
            return await self.connection.fetch(query, *args)

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        """Execute a query with locking to avoid concurrent access issues."""
        async with self.lock:
            return await self.connection.execute(query, *args)

    async def fetchval(
        self,
        query: str,
        *args: Any,
    ) -> Any:
        """Fetch a single value with concurrency protection."""
        async with self.lock:
            return await self.connection.fetchval(query, *args)

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        """Add a database listener with locking to manage concurrency."""
        async with self.lock:
            await self.connection.add_listener(
                channel,
                lambda *x: callback(x[-1]),
            )


class PsycopgDriver:
    def __init__(self, connection: psycopg.AsyncConnection) -> None:
        self.lock = asyncio.Lock()
        self.connection = connection
        self.tm = TaskManager()

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[Any]:
        async with self.lock:
            return await (await self.connection.execute(query, args)).fetchall()

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        async with self.lock:
            return (await self.connection.execute(query, args)).statusmessage or ""

    async def fetchval(
        self,
        query: str,
        *args: Any,
    ) -> Any:
        async with self.lock:
            result = await (await self.connection.execute(query, args)).fetchone()
            return result[0] if result else None

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        assert self.connection.autocommit

        async def notify_handler_wrapper(
            channel: str,
            callback: Callable[[str | bytes | bytearray], None],
        ) -> None:
            await self.execute(f"LISTEN {channel};")

            async for note in self.connection.notifies():
                if note.channel == channel:
                    callback(note.payload)

        def log_exception(x: asyncio.Task) -> None:
            try:
                print(x.result())
            except Exception:
                logger.exception(
                    "Got an exception on notify on channel: %s",
                    channel,
                )

        task = asyncio.create_task(
            notify_handler_wrapper(channel, callback),
            name=f"notify_handler_wrapper_{channel}",
        )
        task.add_done_callback(log_exception)
        self.tm.add(task)
