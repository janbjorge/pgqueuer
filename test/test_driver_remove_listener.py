from __future__ import annotations

import asyncio
import socket
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field

import pytest
from psycopg import sql

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

    async def reset(self) -> None:
        self.listeners.clear()

    def is_closed(self) -> bool:
        return False


@dataclass
class FakePool:
    connection: FakeAsyncpgConnection
    released: int = 0

    def get_max_size(self) -> int:
        return 10

    async def acquire(self) -> FakeAsyncpgConnection:
        return self.connection

    async def release(self, connection: FakeAsyncpgConnection) -> None:
        self.released += 1


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
class FakePgConn:
    """libpq stand-in; the socket pair gives the watcher a real fd to wait on."""

    error: BaseException | None = None
    pending: list[Notify] = field(default_factory=list)
    notify_handler: Callable[[Notify], None] | None = None
    sockets: tuple[socket.socket, socket.socket] = field(default_factory=socket.socketpair)

    @property
    def socket(self) -> int:
        return self.sockets[0].fileno()

    def consume_input(self) -> None:
        if self.error is not None:
            raise self.error
        self.sockets[0].recv(4096)

    def notifies(self) -> Notify | None:
        return self.pending.pop(0) if self.pending else None


@dataclass
class FakePsycopgConnection:
    autocommit: bool = True
    executed: list[str] = field(default_factory=list)
    pgconn: FakePgConn = field(default_factory=FakePgConn)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    handlers: list[Callable[[Notify], None]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.pgconn.notify_handler = self.dispatch

    def dispatch(self, note: Notify) -> None:
        for handler in self.handlers:
            handler(note)

    async def execute(self, query: str | sql.Composable) -> None:
        self.executed.append(query.as_string() if isinstance(query, sql.Composable) else query)

    def add_notify_handler(self, handler: Callable[[Notify], None]) -> None:
        self.handlers.append(handler)

    def remove_notify_handler(self, handler: Callable[[Notify], None]) -> None:
        self.handlers.remove(handler)

    def send(self, note: Notify) -> None:
        self.pgconn.pending.append(note)
        self.pgconn.sockets[1].send(b"x")


@pytest.fixture
def connection() -> Iterator[FakePsycopgConnection]:
    fake = FakePsycopgConnection()
    with fake.pgconn.sockets[0], fake.pgconn.sockets[1]:
        yield fake


async def test_psycopg_remove_listener_stops_watcher(connection: FakePsycopgConnection) -> None:
    """remove_listener cancels the watcher and UNLISTENs once nothing listens (#886)."""
    driver = PsycopgDriver(connection)  # type: ignore[arg-type]
    received: list[str | bytes | bytearray] = []
    await driver.add_listener("ch", received.append)

    connection.send(Notify("ch", "a"))
    await asyncio.sleep(0.05)
    await driver.remove_listener("ch", received.append)
    connection.send(Notify("ch", "b"))
    await asyncio.sleep(0.05)

    assert isinstance(driver, ListenerRemover)
    assert received == ["a"]
    assert not driver._tasks
    assert connection.executed == ['LISTEN "ch"', 'UNLISTEN "ch"']


async def test_asyncpg_pool_keeps_listener_until_last_exit() -> None:
    """A nested exit leaves the shared listener connection in place (#895)."""
    connection = FakeAsyncpgConnection()
    pool = FakePool(connection)
    driver = AsyncpgPoolDriver(pool)  # type: ignore[arg-type]
    received: list[str | bytes | bytearray] = []

    async with driver:
        await driver.add_listener("ch", received.append)
        async with driver:
            pass
        connection.notify("a")
        assert pool.released == 0

    assert received == ["a"]
    assert pool.released == 1


async def test_psycopg_delivers_each_notify_to_its_channel(
    connection: FakePsycopgConnection,
) -> None:
    """Every NOTIFY reaches the callbacks of its own channel, and only those (#894)."""
    driver = PsycopgDriver(connection)  # type: ignore[arg-type]
    on_a: list[str | bytes | bytearray] = []
    on_b: list[str | bytes | bytearray] = []
    await driver.add_listener("a", on_a.append)
    await driver.add_listener("b", on_b.append)

    for note in (Notify("a", "1"), Notify("b", "2"), Notify("a", "3"), Notify("b", "4")):
        connection.send(note)
    await asyncio.sleep(0.1)

    assert on_a == ["1", "3"]
    assert on_b == ["2", "4"]


async def test_psycopg_remove_listener_keeps_other_channels(
    connection: FakePsycopgConnection,
) -> None:
    """Removing one channel's listener leaves other channels receiving."""
    driver = PsycopgDriver(connection)  # type: ignore[arg-type]
    on_b: list[str | bytes | bytearray] = []
    await driver.add_listener("a", print)
    await driver.add_listener("b", on_b.append)

    await driver.remove_listener("a", print)
    connection.send(Notify("b", "1"))
    await asyncio.sleep(0.05)

    assert on_b == ["1"]
    assert connection.executed == ['LISTEN "a"', 'LISTEN "b"', 'UNLISTEN "a"']


async def test_psycopg_keeps_listening_until_last_exit(connection: FakePsycopgConnection) -> None:
    """A nested exit does not stop the shared watcher (#895)."""
    driver = PsycopgDriver(connection)  # type: ignore[arg-type]
    received: list[str | bytes | bytearray] = []

    async with driver:
        await driver.add_listener("ch", received.append)
        async with driver:
            pass
        connection.send(Notify("ch", "a"))
        await asyncio.sleep(0.05)

    assert received == ["a"]


async def test_psycopg_driver_can_be_entered_again(connection: FakePsycopgConnection) -> None:
    """After the last exit, entering again starts listening afresh (#895)."""
    driver = PsycopgDriver(connection)  # type: ignore[arg-type]
    first: list[str | bytes | bytearray] = []
    second: list[str | bytes | bytearray] = []

    async with driver:
        await driver.add_listener("ch", first.append)
    async with driver:
        await driver.add_listener("ch", second.append)
        connection.send(Notify("ch", "a"))
        await asyncio.sleep(0.05)

    assert first == []
    assert second == ["a"]


async def test_psycopg_last_exit_stops_watcher(connection: FakePsycopgConnection) -> None:
    """The last exit stops the notify watcher and removes the notify handler."""
    driver = PsycopgDriver(connection)  # type: ignore[arg-type]
    async with driver:
        await driver.add_listener("ch", lambda _: None)
        await asyncio.sleep(0.05)
        watcher = driver._watcher
        assert watcher is not None
        assert not watcher.done()

    assert watcher.done()
    assert not driver._tasks
    assert not connection.handlers


async def test_psycopg_aexit_swallows_watcher_error(connection: FakePsycopgConnection) -> None:
    """A failed watcher does not raise from the last exit."""
    connection.pgconn.error = RuntimeError("read failed")
    driver = PsycopgDriver(connection)  # type: ignore[arg-type]
    async with driver:
        await driver.add_listener("ch", lambda _: None)
        watcher = driver._watcher
        connection.send(Notify("ch", "a"))
        await asyncio.sleep(0.05)

    assert watcher is not None
    assert watcher.done()
    assert isinstance(watcher.exception(), RuntimeError)
    assert not driver._tasks


async def test_psycopg_two_channels_share_one_watcher(connection: FakePsycopgConnection) -> None:
    """One connection has one watcher; extra LISTENs reuse it."""
    driver = PsycopgDriver(connection)  # type: ignore[arg-type]
    ignored: list[str | bytes | bytearray] = []
    await driver.add_listener("a", ignored.append)
    watcher = driver._watcher
    await driver.add_listener("b", ignored.append)

    assert watcher is not None
    assert driver._watcher is watcher
    assert driver._tasks == {watcher}

    await driver.remove_listener("a", ignored.append)
    await driver.remove_listener("b", ignored.append)
    assert not driver._tasks
