"""
Database abstraction layer for asynchronous PostgreSQL operations.

This module defines the `Driver` protocol and provides implementations for different
asynchronous PostgreSQL drivers, such as AsyncPG and Psycopg. It allows for fetching data,
executing queries, and handling notifications using a consistent interface.
"""

from __future__ import annotations

import asyncio
import functools
import os
import re
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, Protocol

from typing_extensions import Self

from . import tm

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
    """
    Construct a PostgreSQL DSN (Data Source Name) from parameters or environment variables.

    Assembles a PostgreSQL connection string using provided parameters. If any parameter
    is not specified, it attempts to retrieve it from environment variables (`PGHOST`, `PGUSER`,
    `PGPASSWORD`, `PGDATABASE`, `PGPORT`).

    Returns:
        str: A PostgreSQL DSN string in the format 'postgresql://user:password@host:port/database'.
    """
    host = host or os.getenv("PGHOST", "")
    user = user or os.getenv("PGUSER", "")
    password = password or os.getenv("PGPASSWORD", "")
    database = database or os.getenv("PGDATABASE", "")
    port = port or os.getenv("PGPORT", "")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"


class Driver(Protocol):
    """
    Protocol defining the essential database operations for drivers.

    The `Driver` protocol specifies the methods that a database driver must implement
    to be compatible with the system. This includes methods for fetching records,
    executing queries, adding listeners for notifications, and managing the driver's lifecycle.
    """

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        """
        Fetch multiple records from the database.

        Args:
            query (str): The SQL query to execute.
            *args (Any): Positional arguments to substitute into the query.

        Returns:
            list[dict]: A list of dictionaries representing the fetched records.
        """
        raise NotImplementedError

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        """
        Execute a SQL query and return the status message.

        Args:
            query (str): The SQL query to execute.
            *args (Any): Positional arguments to substitute into the query.

        Returns:
            str: The status message returned by the database after execution.
        """
        raise NotImplementedError

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        """
        Add a listener for a PostgreSQL NOTIFY channel.

        Registers a callback function to be called whenever a notification is received
        on the specified channel.

        Args:
            channel (str): The name of the PostgreSQL channel to listen on.
            callback (Callable[[str | bytes | bytearray], None]): The function to call when a
                notification is received.
        """
        raise NotImplementedError

    @property
    def shutdown(self) -> asyncio.Event:
        """
        An asyncio.Event indicating the liveness of the driver.

        This event can be used to signal when the driver is shutting down or no longer active.
        """
        raise NotImplementedError

    @property
    def tm(self) -> tm.TaskManager:
        """
        TaskManager instance for managing background tasks.

        Provides a way to manage and await background tasks associated with the driver.
        """
        raise NotImplementedError

    async def __aenter__(self) -> Self:
        """
        Enter the runtime context related to this object.

        Returns:
            Self: Returns self to allow the driver to be used as an asynchronous context manager.
        """
        raise NotImplementedError

    async def __aexit__(self, *_: object) -> None:
        """
        Exit the runtime context and perform cleanup actions.

        Args:
            *_ (object): Ignored arguments.
        """
        raise NotImplementedError


class AsyncpgDriver(Driver):
    """
    AsyncPG implementation of the `Driver` protocol.

    This driver uses an AsyncPG connection to perform asynchronous database operations
    such as fetching records, executing queries, and listening for notifications.
    It ensures thread safety using an asyncio.Lock.
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
    def tm(self) -> tm.TaskManager:
        return tm.TaskManager()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None: ...


class AsyncpgPoolDriver(Driver):
    """
    Implements the Driver protocol using AsyncPGPool for PostgreSQL database operations.

    This class manages asynchronous database operations using a connection pool.
    It ensures thread safety through query locking and supports PostgreSQL LISTEN/NOTIFY
    functionality with dedicated listeners.
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
    def tm(self) -> tm.TaskManager:
        return tm.TaskManager()

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._listener_connection is not None:
            await self._listener_connection.reset()
            await self._pool.release(self._listener_connection)


@functools.cache
def _replace_dollar_named_parameter(query: str) -> str:
    """
    Replace positional parameters in a SQL query with named parameters.

    This function replaces all occurrences of $1, $2, etc., in the provided SQL query
    string with named parameters of the form %(parameter_1)s, which is compatible with
    Psycopg's named parameter syntax.

    Args:
        query (str): The SQL query string containing positional parameters.

    Returns:
        str: The modified SQL query string with named parameters.
    """
    return re.sub(r"\$(\d+)", r"%(parameter_\1)s", query)


def _named_parameter(args: tuple) -> dict[str, Any]:
    """
    Convert positional arguments into a dictionary of named parameters.

    Creates a dictionary mapping parameter names like 'parameter_1', 'parameter_2', etc.,
    to the provided positional arguments. This is used for parameter substitution in SQL queries.

    Args:
        args (tuple): A tuple of positional arguments.

    Returns:
        dict[str, Any]: A dictionary mapping parameter names to their corresponding values.
    """
    return {f"parameter_{n}": arg for n, arg in enumerate(args, start=1)}


class PsycopgDriver(Driver):
    def __init__(
        self,
        connection: psycopg.AsyncConnection,
        notify_timeout: timedelta = timedelta(seconds=0.25),
        notify_stop_after: int = 10,
    ) -> None:
        self._shutdown = asyncio.Event()
        self._connection = connection
        self._lock = asyncio.Lock()
        self._tm = tm.TaskManager()
        self._notify_stop_after = notify_stop_after
        self._notify_timeout = notify_timeout

    @property
    def shutdown(self) -> asyncio.Event:
        return self._shutdown

    @property
    def tm(self) -> tm.TaskManager:
        return self._tm

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
        if not self._connection.autocommit:
            raise RuntimeError(
                f"Database connection({self._connection}) must have autocommit enabled. This is "
                "required for proper operation of PGQueuer. Ensure that your psycopg connection is "
                "configured with autocommit=True."
            )

        async with self._lock:
            await self._connection.execute(f"LISTEN {channel};")

            async def notify_handler() -> None:
                while not self.shutdown.is_set():
                    gen = self._connection.notifies(
                        timeout=self._notify_timeout.total_seconds(),
                        stop_after=self._notify_stop_after,
                    )
                    async for note in gen:
                        if not self.shutdown.is_set():
                            callback(note.payload)
                    await asyncio.sleep(self._notify_timeout.total_seconds())

            self._tm.add(
                asyncio.create_task(
                    notify_handler(),
                    name=f"notify_psycopg_handler_{channel}",
                )
            )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        self.shutdown.set()
        await self.tm.gather_tasks()
