"""Psqlpy driver implementation."""

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, Callable

from typing_extensions import Self

from pgqueuer.core import logconfig
from pgqueuer.core.tm import TaskManager
from pgqueuer.ports.driver import Driver

if TYPE_CHECKING:
    import psqlpy


def adapt_args(args: tuple[Any, ...]) -> list[Any] | None:
    """Convert query arguments into values psqlpy can bind.

    Psqlpy rejects ``bytes`` inside a sequence -- it reads them as a nested
    array and fails on the element-size mismatch -- so ``bytea[]`` members are
    wrapped in ``CustomType``. Scalars bind natively and pass through.
    """
    if not args:
        return None

    from psqlpy.extra_types import CustomType

    return [
        [CustomType(x) if isinstance(x, bytes) else x for x in arg]
        if isinstance(arg, list)
        else arg
        for arg in args
    ]


class PsqlpyDriver(Driver):
    """Psqlpy implementation of the :class:`Driver` protocol.

    Psqlpy can only build a ``Listener`` from a pool, so this driver wraps a
    ``psqlpy.ConnectionPool`` instead of a single connection. The pool must
    allow at least two connections: the listener holds one for as long as the
    driver is listening.

    The driver does not close the provided pool; callers should manage the
    pool lifecycle themselves.

    Usage example::

        from psqlpy import ConnectionPool
        from pgqueuer.db import PsqlpyDriver

        pool = ConnectionPool(dsn=dsn, max_db_pool_size=5)
        async with PsqlpyDriver(pool) as driver:
            await driver.fetch("SELECT 1")
    """

    def __init__(
        self,
        pool: psqlpy.ConnectionPool,
    ) -> None:
        self._shutdown = asyncio.Event()
        self._pool = pool
        self._lock = asyncio.Lock()
        self._listener: psqlpy.Listener | None = None
        self._callbacks: dict[str, list[Callable[[str | bytes | bytearray], None]]] = {}

        if (max_size := pool.status().max_size) < 2:
            raise RuntimeError(
                f"Pool max size ({max_size}) must be at least 2; the listener holds one "
                "connection, leaving none for queries."
            )

    @property
    def shutdown(self) -> asyncio.Event:
        return self._shutdown

    @property
    def tm(self) -> TaskManager:
        # psqlpy runs its listen loop in the Rust runtime, so the driver owns
        # no asyncio tasks of its own.
        return TaskManager()

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        async with self._pool.acquire() as connection:
            result = await connection.fetch(query, adapt_args(args))
        return result.result()

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        async with self._pool.acquire() as connection:
            if args:
                await connection.execute(query, adapt_args(args))
            else:
                # Prepared statements reject more than one command, so
                # parameterless SQL goes over the simple query protocol.
                await connection.execute_batch(query)
        return ""

    async def notify(self, channel: str, payload: str) -> None:
        await self.execute("SELECT pg_notify($1, $2)", channel, payload)

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        if not channel.isidentifier():
            raise ValueError(f"Invalid channel name: {channel!r}")

        async with self._lock:
            subscribed = channel in self._callbacks
            self._callbacks.setdefault(channel, []).append(callback)
            if not subscribed:
                await self.restart_listener()

    async def stop_listener(self) -> None:
        if self._listener is not None:
            self._listener.abort_listen()
            await self._listener.shutdown()
            self._listener = None

    async def restart_listener(self) -> None:
        """Rebuild the listener so every registered channel is subscribed.

        Psqlpy silently ignores callbacks added after ``listen()`` has started,
        so the whole listener is recreated whenever a channel is added. Callers
        must hold ``self._lock``.
        """
        await self.stop_listener()

        listener = self._pool.listener()
        for channel in self._callbacks:
            await listener.add_callback(channel=channel, callback=self.dispatch)
        await listener.startup()
        listener.listen()
        self._listener = listener

    async def dispatch(
        self,
        connection: psqlpy.Connection,
        payload: str,
        channel: str,
        process_id: int,
    ) -> None:
        # An exception escaping a psqlpy callback stops the listen task for
        # every channel, so callback failures must never propagate.
        for callback in self._callbacks.get(channel, []):
            try:
                callback(payload)
            except Exception:
                logconfig.logger.exception(
                    "Unhandled error in NOTIFY callback for channel %s",
                    channel,
                )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        self.shutdown.set()
        async with self._lock:
            await self.stop_listener()
