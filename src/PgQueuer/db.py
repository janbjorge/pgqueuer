"""Database Abstraction Layer for PgQueuer.

This module provides database driver abstractions and a specific implementation
for AsyncPG to handle database operations asynchronously.
"""

import asyncio
from typing import Any, Callable, Protocol

import asyncpg


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
