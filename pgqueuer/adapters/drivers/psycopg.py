"""Psycopg driver implementations."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Callable

from typing_extensions import Self

from pgqueuer.core.tm import TaskManager
from pgqueuer.ports.driver import Driver, SyncDriver

if TYPE_CHECKING:
    import psycopg


class PsycopgDriver(Driver):
    """Psycopg implementation of the :class:`Driver` protocol.

    This driver operates on an existing ``psycopg.AsyncConnection`` instance to
    execute queries and listen for notifications. It uses ``AsyncRawCursor`` with
    ``dict_row`` row factory to accept PostgreSQL-native ``$1, $2`` placeholders
    directly and return results as dictionaries without manual construction.

    Notifications use ``add_notify_handler`` so callbacks fire during normal
    query processing on the same connection — no dedicated connection or
    background task required.

    The driver does not close the provided connection; callers should
    close it manually or manage it with an async context manager.
    """

    def __init__(
        self,
        connection: psycopg.AsyncConnection,
    ) -> None:
        self._shutdown = asyncio.Event()
        self._connection = connection

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
        return TaskManager()

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        from psycopg import AsyncRawCursor
        from psycopg.rows import dict_row

        cursor = AsyncRawCursor(self._connection, row_factory=dict_row)
        await cursor.execute(query, args or None)
        return await cursor.fetchall()

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        from psycopg import AsyncRawCursor

        cursor = AsyncRawCursor(self._connection)
        await cursor.execute(query, args or None)
        return cursor.statusmessage or ""

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        await self._connection.execute(f"LISTEN {channel};")
        self._connection.add_notify_handler(lambda n: callback(n.payload))

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        self.shutdown.set()


class SyncPsycopgDriver(SyncDriver):
    """Synchronous psycopg implementation of the :class:`SyncDriver` protocol.

    The driver works with an existing ``psycopg.Connection`` instance. It uses
    ``RawCursor`` with ``dict_row`` row factory to accept PostgreSQL-native
    ``$1, $2`` placeholders directly.

    It does not close the provided connection; callers must handle the connection
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
        from psycopg import RawCursor
        from psycopg.rows import dict_row

        cursor = RawCursor(self._connection, row_factory=dict_row)
        cursor.execute(query, args or None)
        return cursor.fetchall()
