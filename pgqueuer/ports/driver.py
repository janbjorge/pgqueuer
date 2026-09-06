"""Port protocols for database drivers."""

from __future__ import annotations

import asyncio
from typing import Callable, Protocol, runtime_checkable

from typing_extensions import Self


@runtime_checkable
class SqlStateError(Protocol):
    """Driver exception carrying a PostgreSQL SQLSTATE code.

    Both asyncpg and psycopg errors expose ``sqlstate``; matching on the
    shape keeps the persistence layer free of driver imports.
    """

    @property
    def sqlstate(self) -> str | None: ...


@runtime_checkable
class ConstraintNamed(Protocol):
    """Exception or diagnostic that names the violated constraint."""

    @property
    def constraint_name(self) -> str | None: ...


@runtime_checkable
class DiagnosticError(Protocol):
    """psycopg-style error whose ``diag`` carries constraint details."""

    @property
    def diag(self) -> object: ...


class TaskManagerPort(Protocol):
    """Protocol for managing background asyncio tasks."""

    tasks: set[asyncio.Task[object]]

    def add(self, task: asyncio.Task[object]) -> None: ...

    async def gather_tasks(self, return_exceptions: bool = True) -> list[object]: ...

    async def __aenter__(self) -> "TaskManagerPort": ...

    async def __aexit__(self, *_: object) -> None: ...


class Driver(Protocol):
    """Async database driver contract: fetch/execute/notify/listen + lifecycle."""

    async def fetch(
        self,
        query: str,
        *args: object,
    ) -> list[dict[str, object]]:
        raise NotImplementedError

    async def execute(
        self,
        query: str,
        *args: object,
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
        *args: object,
    ) -> list[dict[str, object]]:
        raise NotImplementedError
