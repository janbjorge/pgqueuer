from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from typing import AsyncGenerator, Callable

import pytest

from pgqueuer.adapters.drivers.asyncpg import AsyncpgDriver, AsyncpgPoolDriver
from pgqueuer.adapters.drivers.psycopg import PsycopgDriver
from pgqueuer.ports.driver import ListenerRemover


@dataclass
class FakeAsyncpgConnection:
    """Keeps listeners like asyncpg does: removed by equality."""

    listeners: list[Callable[..., None]] = field(default_factory=list)

    async def add_listener(self, channel: str, callback: Callable[..., None]) -> None:
        self.listeners.append(callback)

    async def remove_listener(self, channel: str, callback: Callable[..., None]) -> None:
        self.listeners.remove(callback)

    def notify(self, payload: str) -> None:
        for listener in list(self.listeners):
            listener(self, 1, "ch", payload)


@dataclass
class FakePool:
    connection: FakeAsyncpgConnection

    def get_max_size(self) -> int:
        return 10

    async def acquire(self) -> FakeAsyncpgConnection:
        return self.connection


@pytest.mark.parametrize("pool", (False, True))
async def test_asyncpg_remove_listener_keeps_other_callbacks(pool: bool) -> None:
    """remove_listener unhooks only the given callback (#886)."""
    connection = FakeAsyncpgConnection()
    driver = (
        AsyncpgPoolDriver(FakePool(connection))  # type: ignore[arg-type]
        if pool
        else AsyncpgDriver(connection)  # type: ignore[arg-type]
    )
    removed: list[str | bytes | bytearray] = []
    kept: list[str | bytes | bytearray] = []
    await driver.add_listener("ch", removed.append)
    await driver.add_listener("ch", kept.append)

    connection.notify("a")
    await driver.remove_listener("ch", removed.append)
    connection.notify("b")

    assert isinstance(driver, ListenerRemover)
    assert removed == ["a"]
    assert kept == ["a", "b"]


@dataclass
class Notify:
    channel: str
    payload: str


@dataclass
class FakePsycopgConnection:
    autocommit: bool = True
    executed: list[str] = field(default_factory=list)
    notes: asyncio.Queue[Notify] = field(default_factory=asyncio.Queue)

    async def execute(self, query: str) -> None:
        self.executed.append(query)

    async def notifies(self, *, timeout: float, stop_after: int) -> AsyncGenerator[Notify, None]:
        with suppress(TimeoutError, asyncio.TimeoutError):
            yield await asyncio.wait_for(self.notes.get(), timeout)


async def test_psycopg_remove_listener_stops_watcher() -> None:
    """remove_listener cancels the watcher and UNLISTENs once nothing listens (#886)."""
    connection = FakePsycopgConnection()
    driver = PsycopgDriver(connection)  # type: ignore[arg-type]
    received: list[str | bytes | bytearray] = []
    await driver.add_listener("ch", received.append)

    await connection.notes.put(Notify("ch", "a"))
    await asyncio.sleep(0.05)
    await driver.remove_listener("ch", received.append)
    await connection.notes.put(Notify("ch", "b"))
    await asyncio.sleep(0.05)

    assert isinstance(driver, ListenerRemover)
    assert received == ["a"]
    assert not driver.tm.tasks
    assert connection.executed == ["LISTEN ch", "UNLISTEN ch"]
