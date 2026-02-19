"""In-memory driver satisfying the Driver protocol.

Stores listener callbacks and provides ``deliver()`` to push
notifications without PostgreSQL.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import Any, Callable

from typing_extensions import Self

from pgqueuer.core.tm import TaskManager


class InMemoryDriver:
    """Drop-in replacement for PostgreSQL-backed drivers.

    ``fetch()`` and ``execute()`` raise ``NotImplementedError`` because
    ``InMemoryQueries`` handles data directly — they are never called.
    """

    def __init__(self) -> None:
        self._shutdown = asyncio.Event()
        self._tm = TaskManager()
        self._listeners: dict[str, list[Callable[[str], None]]] = defaultdict(list)

    # -- Driver protocol -------------------------------------------------------

    async def fetch(self, query: str, *args: Any) -> list[dict]:
        raise NotImplementedError("InMemoryDriver does not support SQL fetch")

    async def execute(self, query: str, *args: Any) -> str:
        raise NotImplementedError("InMemoryDriver does not support SQL execute")

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str], None],
    ) -> None:
        self._listeners[channel].append(callback)

    @property
    def shutdown(self) -> asyncio.Event:
        return self._shutdown

    @property
    def tm(self) -> TaskManager:
        return self._tm

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_: object) -> None:
        pass

    # -- In-memory notification ------------------------------------------------

    def deliver(self, channel: str, payload: str) -> None:
        """Push *payload* to all registered listeners on *channel*.

        This is the in-memory equivalent of ``pg_notify(channel, payload)``.
        """
        for cb in self._listeners.get(channel, ()):
            cb(payload)
