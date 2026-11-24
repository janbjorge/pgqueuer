"""Psycopg driver implementations."""

from __future__ import annotations

import asyncio
import os
from typing import TYPE_CHECKING, Self

from pgqueuer.core import logconfig
from pgqueuer.core.tm import cancel_and_wait
from pgqueuer.ports.driver import Driver, NotifyCallback, SyncDriver

if TYPE_CHECKING:
    import psycopg


class PsycopgDriver(Driver):
    """Psycopg implementation of the :class:`Driver` protocol.

    This driver operates on an existing ``psycopg.AsyncConnection`` instance to
    execute queries and listen for notifications. It uses ``AsyncRawCursor`` with
    ``dict_row`` row factory to accept PostgreSQL-native ``$1, $2`` placeholders
    directly and return results as dictionaries without manual construction.

    A psycopg notify handler gives each NOTIFY to the callbacks of its channel.
    psycopg calls the handler for each NOTIFY that it reads, also during a query.
    When the connection is idle, a watcher task waits until the socket is readable.
    The watcher does not hold the connection lock while it waits. Thus, queries
    do not wait for the watcher.

    The driver does not close the provided connection; callers should
    close it manually or manage it with an async context manager.
    """

    def __init__(
        self,
        connection: psycopg.AsyncConnection,
    ) -> None:
        self._shutdown = asyncio.Event()
        self._connection = connection
        self._tasks: set[asyncio.Task[None]] = set()
        self._listeners: dict[str, list[NotifyCallback]] = {}
        self._watcher: asyncio.Task[None] | None = None
        self._entered = 0

        if not self._connection.autocommit:
            raise RuntimeError(
                f"Database connection({self._connection}) must have autocommit enabled. This is "
                "required for proper operation of PgQueuer. Ensure that your psycopg connection is "
                "configured with autocommit=True."
            )

    @property
    def shutdown(self) -> asyncio.Event:
        return self._shutdown

    async def fetch(
        self,
        query: str,
        *args: object,
    ) -> list[dict[str, object]]:
        from psycopg import AsyncRawCursor
        from psycopg.rows import dict_row

        cursor = AsyncRawCursor(self._connection, row_factory=dict_row)
        await cursor.execute(query, args or None)
        return await cursor.fetchall()

    async def execute(
        self,
        query: str,
        *args: object,
    ) -> str:
        from psycopg import AsyncRawCursor

        cursor = AsyncRawCursor(self._connection)
        await cursor.execute(query, args or None)
        return cursor.statusmessage or ""

    async def notify(self, channel: str, payload: str) -> None:
        await self.execute("SELECT pg_notify($1, $2)", channel, payload)

    async def add_listener(
        self,
        channel: str,
        callback: NotifyCallback,
    ) -> None:
        from psycopg import sql

        if channel not in self._listeners:
            # Quote like asyncpg's add_listener does: an unquoted LISTEN folds
            # the name to lowercase, so it would never match the trigger's
            # pg_notify('{channel}') literal for mixed-case channels (#947).
            await self._connection.execute(sql.SQL("LISTEN {}").format(sql.Identifier(channel)))
            self._listeners[channel] = []
        self._listeners[channel].append(callback)

        if self._watcher is None:
            self._connection.add_notify_handler(self._dispatch)
            self._watcher = asyncio.create_task(
                self.watch_notifies(), name="notify_psycopg_watcher"
            )
            self._tasks.add(self._watcher)
            self._watcher.add_done_callback(self._tasks.discard)

    def _dispatch(self, note: psycopg.Notify) -> None:
        """Give the NOTIFY to each callback of its channel."""
        for callback in list(self._listeners.get(note.channel, [])):
            try:
                callback(note.payload)
            except Exception:
                logconfig.logger.exception(
                    "Unhandled error in NOTIFY callback for channel %s",
                    note.channel,
                )

    async def watch_notifies(self) -> None:
        """Read the NOTIFY messages that arrive when the connection is idle.

        The watcher waits on a copy of the socket. psycopg waits on the socket
        itself, and asyncio keeps only one reader for each file descriptor.
        """
        loop = asyncio.get_running_loop()
        pgconn = self._connection.pgconn
        fd = os.dup(pgconn.socket)
        try:
            while True:
                readable = loop.create_future()
                loop.add_reader(fd, readable.set_result, None)
                try:
                    await readable
                finally:
                    # Remove the reader. A socket that stays readable must not wake the loop again.
                    loop.remove_reader(fd)
                # A query can hold the lock. That query then reads the data itself.
                async with self._connection.lock:
                    pgconn.consume_input()
                    while note := pgconn.notifies():
                        if pgconn.notify_handler is not None:
                            pgconn.notify_handler(note)
        finally:
            os.close(fd)

    async def remove_listener(
        self,
        channel: str,
        callback: NotifyCallback,
    ) -> None:
        from psycopg import sql

        callbacks = self._listeners.get(channel, [])
        if callback not in callbacks:
            return
        callbacks.remove(callback)
        if callbacks:
            return

        del self._listeners[channel]
        if not self._listeners:
            await self._stop_watcher()
        await self._connection.execute(sql.SQL("UNLISTEN {}").format(sql.Identifier(channel)))

    async def _stop_watcher(self) -> None:
        """Stop the watcher and remove the notify handler."""
        if self._watcher is None:
            return
        self._connection.remove_notify_handler(self._dispatch)
        await cancel_and_wait(self._watcher)
        self._watcher = None

    async def __aenter__(self) -> Self:
        if self._entered == 0:
            self._shutdown.clear()
        self._entered += 1
        return self

    async def __aexit__(self, *_: object) -> None:
        # PgQueuer.run enters one driver from two managers; only the last exit may stop listening.
        self._entered -= 1
        if self._entered > 0:
            return
        self.shutdown.set()
        await self._stop_watcher()
        # Start afresh on the next enter: add_listener then LISTENs and starts a new watcher.
        self._listeners.clear()


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
                "required for proper operation of PgQueuer. Ensure that your psycopg connection is "
                "configured with autocommit=True."
            )

    def fetch(
        self,
        query: str,
        *args: object,
    ) -> list[dict[str, object]]:
        from psycopg import RawCursor
        from psycopg.rows import dict_row

        cursor = RawCursor(self._connection, row_factory=dict_row)
        cursor.execute(query, args or None)
        return cursor.fetchall()
