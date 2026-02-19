"""Lightweight in-memory Driver stub.

Satisfies the ``Driver`` protocol (``pgqueuer/ports/driver.py``) without any
database connection.  The ``_listeners`` dict is the bridge between the
repository and the event system: the repository pushes serialised event JSON
through these callbacks so that ``PGNoticeEventListener`` receives events
exactly as if they came from PostgreSQL.
"""

from __future__ import annotations

import asyncio
import dataclasses
from typing import Any, Callable

from pgqueuer.core.tm import TaskManager


@dataclasses.dataclass
class InMemoryDriver:
    """In-memory driver that satisfies the ``Driver`` protocol."""

    _listeners: dict[str, list[Callable[[str | bytes | bytearray], None]]] = (
        dataclasses.field(default_factory=dict)
    )
    _shutdown: asyncio.Event = dataclasses.field(default_factory=asyncio.Event)
    _tm: TaskManager = dataclasses.field(default_factory=TaskManager)

    async def fetch(self, query: str, *args: Any) -> list[dict]:
        return []

    async def execute(self, query: str, *args: Any) -> str:
        return ""

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        self._listeners.setdefault(channel, []).append(callback)

    @property
    def shutdown(self) -> asyncio.Event:
        return self._shutdown

    @property
    def tm(self) -> TaskManager:
        return self._tm

    async def __aenter__(self) -> "InMemoryDriver":
        return self

    async def __aexit__(self, *_: object) -> None:
        pass
