"""Port protocols for database drivers.

These Protocol classes define the contracts for database access.
"""

from __future__ import annotations

import asyncio
from typing import Any, Callable, Protocol

from typing_extensions import Self


class TaskManagerPort(Protocol):
    """Protocol for managing background asyncio tasks."""

    tasks: set[asyncio.Task]

    def add(self, task: asyncio.Task) -> None: ...

    async def gather_tasks(self, return_exceptions: bool = True) -> list[BaseException | None]: ...

    async def __aenter__(self) -> "TaskManagerPort": ...

    async def __aexit__(self, *_: object) -> None: ...


class Driver(Protocol):
    """Async database driver contract: fetch/execute/notify/listen + lifecycle."""

    async def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        raise NotImplementedError

    async def execute(
        self,
        query: str,
        *args: Any,
    ) -> str:
        raise NotImplementedError

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        """Register *callback* to receive payloads from NOTIFY on *channel*."""
        raise NotImplementedError

    async def notify(self, channel: str, payload: str) -> None:
        """Send a NOTIFY on *channel* with *payload*."""
        raise NotImplementedError

    @property
    def shutdown(self) -> asyncio.Event:
        """Set when the driver should stop accepting new work."""
        raise NotImplementedError

    @property
    def tm(self) -> TaskManagerPort:
        """TaskManager tracking the driver's background tasks (e.g. notify watchers)."""
        raise NotImplementedError

    async def __aenter__(self) -> Self:
        raise NotImplementedError

    async def __aexit__(self, *_: object) -> None:
        raise NotImplementedError


class SyncDriver(Protocol):
    """Synchronous database driver contract: fetch only."""

    def fetch(
        self,
        query: str,
        *args: Any,
    ) -> list[dict]:
        raise NotImplementedError
