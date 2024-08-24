"""Database Abstraction Layer for pgqueuer.

This module provides database driver abstractions and a specific implementation
for AsyncPG to handle database operations asynchronously.
"""

from __future__ import annotations

import asyncio
import functools
import os
import re
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, Protocol

from .logconfig import logger

if TYPE_CHECKING:
    import asyncpg
    import psycopg


def dsn(
    host: str = "",
    user: str = "",
    password: str = "",
    database: str = "",
    port: str = "",
) -> str:
    host = host or os.getenv("PGHOST", "")
    user = user or os.getenv("PGUSER", "")
    password = password or os.getenv("PGPASSWORD", "")
    database = database or os.getenv("PGDATABASE", "")
    port = port or os.getenv("PGPORT", "")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


class Driver(Protocol):
    """
    Defines a protocol for database drivers with essential database operations.
    """

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
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

    @property
    def alive(self) -> bool:
        raise NotImplementedError

    @alive.setter
    def alive(self, value: bool) -> None:
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
        self._alive = True
        self._connection = connection
        self._lock = asyncio.Lock()

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        """Fetch records with query locking to ensure thread safety."""
        async with self._lock:
            return [dict(x) for x in await self._connection.fetch(query, *args)]

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        """Execute a query with locking to avoid concurrent access issues."""
        async with self._lock:
            return await self._connection.execute(query, *args)

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        """Add a database listener with locking to manage concurrency."""
        async with self._lock:
            await self._connection.add_listener(
                channel,
                lambda *x: callback(x[-1]),
            )

    @property
    def alive(self) -> bool:
        return self._alive

    @alive.setter
    def alive(self, value: bool) -> None:
        self._alive = value


@functools.cache
def _replace_dollar_named_parameter(query: str) -> str:
    """
    Replaces all instances of $1, $2, etc. with %(parameter_1)s in a
    given SQL query string.
    """
    return re.sub(r"\$(\d+)", r"%(parameter_\1)s", query)


def _named_parameter(args: tuple) -> dict[str, Any]:
    return {f"parameter_{n}": arg for n, arg in enumerate(args, start=1)}


class PsycopgDriver:
    def __init__(
        self,
        connection: psycopg.AsyncConnection,
        notify_timeout: timedelta = timedelta(seconds=0.25),
        notify_stop_after: int = 10,
    ) -> None:
        self._alive = True
        self._callbacks: dict[str, Callable[[str | bytes | bytearray], None]] = {}
        self._connection = connection
        self._lock = asyncio.Lock()
        self._notify_handler_task: None | asyncio.Task = None
        self._notify_stop_after = notify_stop_after
        self._notify_timeout = notify_timeout

    @property
    def alive(self) -> bool:
        return self._alive

    @alive.setter
    def alive(self, value: bool) -> None:
        self._alive = value

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        cursor = await self._connection.execute(
            _replace_dollar_named_parameter(query),
            _named_parameter(args),
        )
        if not (description := cursor.description):
            raise RuntimeError("No description")
        cols = [col.name for col in description]
        return [dict(zip(cols, val)) for val in await cursor.fetchall()]

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        return (
            await self._connection.execute(
                _replace_dollar_named_parameter(query),
                _named_parameter(args),
            )
        ).statusmessage or ""

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        assert self._connection.autocommit
        async with self._lock:
            await self._connection.execute(f"LISTEN {channel};")
            self._callbacks[channel] = callback

            if self._notify_handler_task is not None:
                return

            async def notify_handler() -> None:
                while self._alive:
                    gen = self._connection.notifies(
                        timeout=self._notify_timeout.total_seconds(),
                        stop_after=self._notify_stop_after,
                    )
                    async for note in gen:
                        if cb := self._callbacks.get(note.channel):
                            cb(note.payload)
                    await asyncio.sleep(self._notify_timeout.total_seconds())

            def log_exception(x: asyncio.Task) -> None:
                try:
                    x.result()
                except asyncio.exceptions.CancelledError:
                    ...
                except Exception:
                    logger.exception(
                        "Got an exception on notify on channel: %s",
                        channel,
                    )

            self._notify_handler_task = asyncio.create_task(
                notify_handler(),
                name="notify_handler",
            )
            self._notify_handler_task.add_done_callback(
                log_exception,
            )
