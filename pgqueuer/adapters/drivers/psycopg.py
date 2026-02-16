"""Psycopg driver implementations."""

from __future__ import annotations

import asyncio
import functools
import re
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable

from typing_extensions import Self

from pgqueuer.core.tm import TaskManager
from pgqueuer.ports.driver import Driver, SyncDriver

if TYPE_CHECKING:
    import psycopg


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
    """Psycopg implementation of the :class:`Driver` protocol.

    This driver operates on an existing ``psycopg.AsyncConnection`` instance to
    execute queries and listen for notifications. The driver itself does not
    close the provided connection; callers should close it manually or manage it
    with an async context manager.
    """

    def __init__(
        self,
        connection: psycopg.AsyncConnection,
        notify_timeout: timedelta = timedelta(seconds=0.25),
        notify_stop_after: int = 10,
    ) -> None:
        self._shutdown = asyncio.Event()
        self._connection = connection
        self._lock = asyncio.Lock()
        self._tm = TaskManager()
        self._notify_stop_after = notify_stop_after
        self._notify_timeout = notify_timeout

        if not self._connection.autocommit:
            raise RuntimeError(
                f"Database connection({self._connection}) must have autocommit enabled. This is "
                "required for proper operation of PGQueuer. Ensure that your psycopg connection is "
                "configured with autocommit=True."
            )

    @property
    def shutdown(self) -> asyncio.Event:
        return self._shutdown

    @property
    def tm(self) -> TaskManager:
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


class SyncPsycopgDriver(SyncDriver):
    """Synchronous psycopg implementation of the :class:`SyncDriver` protocol.

    The driver works with an existing ``psycopg.Connection`` instance. It does
    not close the provided connection; callers must handle the connection
    lifecycle themselves.
    """

    def __init__(
        self,
        connection: psycopg.Connection,
    ) -> None:
        self._connection = connection
        if not self._connection.autocommit:
            raise RuntimeError(
                f"Database connection({self._connection}) must have autocommit enabled. This is "
                "required for proper operation of PGQueuer. Ensure that your psycopg connection is "
                "configured with autocommit=True."
            )

    def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        cursor = self._connection.execute(
            _replace_dollar_named_parameter(query),
            _named_parameter(args),
        )
        if not (description := cursor.description):
            raise RuntimeError("No description")
        cols = [col.name for col in description]
        return [dict(zip(cols, val)) for val in cursor.fetchall()]
