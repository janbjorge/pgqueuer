"""The core listens through the repository, with a driver fallback for old repositories."""

from __future__ import annotations

import asyncio
import dataclasses
import warnings
from datetime import timedelta
from typing import cast

import pytest

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.core.completion import CompletionWatcher
from pgqueuer.core.listeners import DriverListener, listener_for
from pgqueuer.core.qm import QueueManager
from pgqueuer.ports import RepositoryPort
from pgqueuer.ports.driver import NotifyCallback


@dataclasses.dataclass
class RecordingQueries(InMemoryQueries):
    calls: list[tuple[str, str]] = dataclasses.field(default_factory=list)

    async def add_listener(self, channel: str, callback: NotifyCallback) -> None:
        self.calls.append(("add", channel))
        await super().add_listener(channel, callback)

    async def remove_listener(self, channel: str, callback: NotifyCallback) -> None:
        self.calls.append(("remove", channel))
        await super().remove_listener(channel, callback)


class OldRepository:
    """A custom repository from before NotificationListener."""

    def __init__(self, inner: InMemoryQueries) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> object:
        if name in {"add_listener", "remove_listener"}:
            raise AttributeError(name)
        return getattr(self._inner, name)


async def run_until_listening(qm: QueueManager) -> None:
    task = asyncio.create_task(qm.run(dequeue_timeout=timedelta(seconds=0.1)))
    await asyncio.wait_for(qm.listening.wait(), timeout=5)
    await qm.listener_healthy(timeout=timedelta(seconds=1))
    qm.shutdown.set()
    await asyncio.wait_for(task, timeout=5)


def test_listener_for_returns_a_repository_that_listens(queries: InMemoryQueries) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        assert listener_for(queries) is queries


def test_listener_for_falls_back_to_the_driver(queries: InMemoryQueries) -> None:
    old = cast(RepositoryPort, OldRepository(queries))
    with pytest.warns(DeprecationWarning, match="OldRepository has no add_listener"):
        listener = listener_for(old)
    assert isinstance(listener, DriverListener)
    assert listener.driver is queries.driver


async def test_driver_listener_skips_remove_without_listener_remover() -> None:
    class AddOnlyDriver:
        async def add_listener(self, channel: str, callback: NotifyCallback) -> None: ...

    listener = DriverListener(cast(InMemoryDriver, AddOnlyDriver()))
    await listener.remove_listener("ch", lambda _: None)


async def test_qm_listens_through_the_repository(driver: InMemoryDriver) -> None:
    queries = RecordingQueries(driver=driver)
    qm = QueueManager(queries)
    await run_until_listening(qm)
    assert queries.calls == [("add", qm.channel), ("remove", qm.channel)]
    assert not driver._listeners[qm.channel]


async def test_qm_still_listens_through_the_driver_of_an_old_repository(
    queries: InMemoryQueries,
) -> None:
    qm = QueueManager(cast(RepositoryPort, OldRepository(queries)))
    with pytest.warns(DeprecationWarning, match="listens through its driver"):
        await run_until_listening(qm)
    assert not queries.driver._listeners[qm.channel]


async def test_completion_watcher_listens_through_the_repository(driver: InMemoryDriver) -> None:
    queries = RecordingQueries(driver=driver)
    channel = queries.settings.channel
    async with CompletionWatcher(driver, queries=queries, refresh_interval=None):
        assert queries.calls == [("add", channel)]
    assert queries.calls == [("add", channel), ("remove", channel)]
