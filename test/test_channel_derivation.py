"""The LISTEN channel must follow the queries' settings, not a separate default.

Regression tests for three bugs with custom prefixes/channels:

- QueueManager/PgQueuer evaluated their channel default at import time from an
  independent DBSettings(), so LISTEN and NOTIFY could disagree and workers
  never woke.
- CompletionWatcher listened on a hardcoded default channel, ignoring the
  settings of its injected queries, so wait_for() never resolved via NOTIFY.
- ``pgq listen`` resolved its --channel default at import, before --prefix was
  exported to the environment.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Callable

import async_timeout
import pytest

from pgqueuer import PgQueuer, db
from pgqueuer.adapters.inmemory.driver import InMemoryDriver
from pgqueuer.core.completion import CompletionWatcher
from pgqueuer.core.qm import QueueManager
from pgqueuer.domain import errors
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import Channel, QueueExecutionMode
from pgqueuer.models import Job
from pgqueuer.queries import Queries
from test.helpers import queries_for


class RecorderDriver(InMemoryDriver):
    """InMemoryDriver that records LISTEN channels and tolerates SQL calls."""

    def __init__(self) -> None:
        super().__init__()
        self.listened_channels: list[str] = []

    async def fetch(self, query: str, *args: object) -> list[dict]:
        return []

    async def execute(self, query: str, *args: object) -> str:
        return ""

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str], None],
    ) -> None:
        self.listened_channels.append(channel)
        await super().add_listener(channel, callback)


def test_queue_manager_channel_derives_from_queries_settings() -> None:
    settings = DBSettings(prefix="custom_")
    qm = QueueManager(queries_for(RecorderDriver(), settings))
    assert str(qm.channel) == "custom_ch_pgqueuer"


def test_queue_manager_explicit_channel_still_wins() -> None:
    settings = DBSettings(prefix="custom_")
    qm = QueueManager(queries_for(RecorderDriver(), settings), Channel("explicit_ch"))
    assert str(qm.channel) == "explicit_ch"


def test_pgqueuer_channel_derives_from_queries_settings() -> None:
    settings = DBSettings(prefix="custom_")
    pgq = PgQueuer(RecorderDriver(), queries=queries_for(RecorderDriver(), settings))
    assert str(pgq.channel) == "custom_ch_pgqueuer"
    assert pgq.channel == pgq.qm.channel


def test_channel_defaults_are_not_frozen_at_import_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Env vars set after import must reach the default channel."""
    monkeypatch.setenv("PGQUEUER_PREFIX", "zz_")
    assert str(QueueManager(Queries(RecorderDriver())).channel) == "zz_ch_pgqueuer"
    assert str(PgQueuer(RecorderDriver()).channel) == "zz_ch_pgqueuer"


async def test_completion_watcher_listens_on_the_queries_channel() -> None:
    settings = DBSettings(prefix="custom_")
    driver = RecorderDriver()
    async with CompletionWatcher(driver, queries=queries_for(driver, settings)):
        pass
    assert driver.listened_channels == ["custom_ch_pgqueuer"]


async def test_queue_manager_listener_round_trip_under_custom_prefix(
    apgdriver: db.Driver,
) -> None:
    """LISTEN and NOTIFY agree under a custom prefix (fails on the split-channel bug)."""
    await Queries(apgdriver).uninstall()
    settings = DBSettings(prefix="iso_")
    queries = queries_for(apgdriver, settings)
    await queries.install()

    qm = QueueManager(queries)

    @qm.entrypoint("noop")
    async def noop(job: Job) -> None: ...

    run_task = asyncio.create_task(qm.run(dequeue_timeout=timedelta(seconds=0.1)))
    try:
        async with async_timeout.timeout(10):
            while True:
                try:
                    await qm.listener_healthy(timeout=timedelta(seconds=1))
                    break
                except errors.FailingListenerError:
                    await asyncio.sleep(0.05)
    finally:
        qm.shutdown.set()
        async with async_timeout.timeout(10):
            await run_task


async def test_completion_watcher_resolves_under_custom_prefix(
    apgdriver: db.Driver,
) -> None:
    """wait_for resolves via NOTIFY under a custom prefix (fails on the hardcoded channel).

    The safety-net poll is set far above the timeout so only the LISTEN path
    can resolve the future.
    """
    await Queries(apgdriver).uninstall()
    settings = DBSettings(prefix="iso_")
    queries = queries_for(apgdriver, settings)
    await queries.install()

    qm = QueueManager(queries)

    @qm.entrypoint("ep")
    async def ep(job: Job) -> None: ...

    (jid,) = await queries.enqueue("ep", None, 0)

    async with (
        async_timeout.timeout(10),
        CompletionWatcher(
            apgdriver,
            queries=queries,
            refresh_interval=timedelta(seconds=60),
        ) as watcher,
    ):
        fut = watcher.wait_for(jid)
        # Let the debounce check armed by wait_for run while the job is still
        # queued; after this, only a NOTIFY on the right channel can resolve.
        await asyncio.sleep(0.2)
        assert not fut.done()
        await qm.run(mode=QueueExecutionMode.drain)
        async with async_timeout.timeout(2):
            assert await fut == "successful"


def test_cli_listen_honors_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    """Regression: the --channel default was evaluated at import, before --prefix."""
    import contextlib
    from typing import AsyncGenerator

    from typer.testing import CliRunner

    from pgqueuer.adapters.cli import cli
    from pgqueuer.adapters.persistence import qb

    captured: list[str] = []

    @contextlib.asynccontextmanager
    async def fake_yield_queries(
        ctx: object,
        settings: qb.DBSettings,
    ) -> AsyncGenerator[Queries, None]:
        yield queries_for(RecorderDriver(), settings)

    async def fake_display_pg_channel(connection: object, channel: Channel) -> None:
        captured.append(str(channel))

    monkeypatch.setattr(cli, "yield_queries", fake_yield_queries)
    monkeypatch.setattr(cli, "display_pg_channel", fake_display_pg_channel)
    monkeypatch.delenv("PGQUEUER_PREFIX", raising=False)

    result = CliRunner().invoke(cli.app, ["--prefix", "foo_", "listen"])
    assert result.exit_code == 0, result.output
    assert captured == ["foo_ch_pgqueuer"]

    captured.clear()
    result = CliRunner().invoke(cli.app, ["--prefix", "foo_", "listen", "--channel", "explicit_ch"])
    assert result.exit_code == 0, result.output
    assert captured == ["explicit_ch"]


def test_mcp_server_settings_default_is_resolved_at_startup() -> None:
    import inspect

    from pgqueuer.adapters.mcp.server import create_mcp_server

    default = inspect.signature(create_mcp_server).parameters["settings"].default
    assert default is None
