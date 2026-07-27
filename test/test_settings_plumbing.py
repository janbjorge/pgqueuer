"""Settings plumbing: one DBSettings instance flows from the composition root.

Locks the single-source-of-truth invariant introduced with ``Queries(driver,
settings=...)``: builders derive from the queries' settings, the LISTEN
channel always matches the NOTIFY channel, and nothing reads PGQUEUER_* env
vars at import time.
"""

from __future__ import annotations

import asyncio
import dataclasses
import inspect
import warnings
from datetime import timedelta
from typing import Callable

import async_timeout
import pytest

from pgqueuer import PgQueuer, Queries, db
from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.adapters.persistence.queries import SyncQueries
from pgqueuer.core.completion import CompletionWatcher
from pgqueuer.core.qm import QueueManager
from pgqueuer.core.tm import TaskManager
from pgqueuer.domain import errors
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import Channel, QueueExecutionMode
from pgqueuer.models import Job


class RecorderDriver:
    """Driver stub recording LISTEN channels; queries never hit a database."""

    def __init__(self) -> None:
        self.listened_channels: list[str] = []
        self._shutdown = asyncio.Event()
        self._tm = TaskManager()

    async def fetch(self, query: str, *args: object) -> list[dict]:
        return []

    async def execute(self, query: str, *args: object) -> str:
        return ""

    async def add_listener(
        self,
        channel: str,
        callback: Callable[[str | bytes | bytearray], None],
    ) -> None:
        self.listened_channels.append(channel)

    async def notify(self, channel: str, payload: str) -> None:
        pass

    @property
    def shutdown(self) -> asyncio.Event:
        return self._shutdown

    @property
    def tm(self) -> TaskManager:
        return self._tm

    async def __aenter__(self) -> RecorderDriver:
        return self

    async def __aexit__(self, *_: object) -> None:
        pass


class SyncRecorderDriver:
    """SyncDriver stub; SyncQueries never issues queries in these tests."""

    def fetch(self, query: str, *args: object) -> list[dict]:
        return []


def assert_single_settings_instance(
    queries: Queries | SyncQueries | InMemoryQueries,
    settings: DBSettings,
) -> None:
    """Every field holding a settings-bearing object must share *settings*.

    Iterating fields (rather than naming qbe/qbq/qbs) makes this cover any
    future builder automatically.
    """
    builders_checked = 0
    for field in dataclasses.fields(queries):
        value = getattr(queries, field.name)
        if value is settings:
            continue
        if hasattr(value, "settings"):
            assert value.settings is settings, f"{field.name} holds a different DBSettings"
            builders_checked += 1
    assert builders_checked > 0, "no builders found; invariant test is vacuous"


def test_queries_builders_share_the_settings_instance() -> None:
    settings = DBSettings(prefix="custom_")
    assert_single_settings_instance(Queries(RecorderDriver(), settings=settings), settings)


def test_sync_queries_builders_share_the_settings_instance() -> None:
    settings = DBSettings(prefix="custom_")
    assert_single_settings_instance(SyncQueries(SyncRecorderDriver(), settings=settings), settings)


def test_inmemory_queries_builders_share_the_settings_instance() -> None:
    settings = DBSettings(prefix="custom_")
    assert_single_settings_instance(
        InMemoryQueries(driver=InMemoryDriver(), settings=settings),
        settings,
    )


def test_queue_manager_channel_derives_from_queries_settings() -> None:
    settings = DBSettings(prefix="custom_")
    qm = QueueManager(Queries(RecorderDriver(), settings=settings))
    assert qm.channel == settings.channel
    assert str(qm.channel) == "custom_ch_pgqueuer"


def test_queue_manager_channel_param_is_deprecated_but_works_when_matching() -> None:
    settings = DBSettings(prefix="custom_")
    queries = Queries(RecorderDriver(), settings=settings)
    with pytest.warns(DeprecationWarning, match="channel= is deprecated"):
        qm = QueueManager(queries, settings.channel)
    assert qm.channel == settings.channel


def test_queue_manager_conflicting_channel_raises() -> None:
    queries = Queries(RecorderDriver(), settings=DBSettings(prefix="custom_"))
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        with pytest.raises(ValueError, match="conflicts with the queries' settings"):
            QueueManager(queries, Channel("some_other_channel"))


def test_pgqueuer_threads_settings_into_queries_and_channel() -> None:
    settings = DBSettings(prefix="custom_")
    pgq = PgQueuer(RecorderDriver(), settings=settings)
    assert isinstance(pgq.queries, Queries)
    assert pgq.queries.settings is settings
    assert pgq.channel == settings.channel
    assert pgq.qm.channel == settings.channel


def test_pgqueuer_rejects_settings_and_queries_together() -> None:
    driver = RecorderDriver()
    queries = Queries(driver, settings=DBSettings(prefix="a_"))
    with pytest.raises(ValueError, match="mutually exclusive"):
        PgQueuer(driver, queries=queries, settings=DBSettings(prefix="b_"))


def test_pgqueuer_default_construction_emits_no_deprecation_warning() -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("error", DeprecationWarning)
        PgQueuer(RecorderDriver())


def test_pgqueuer_channel_param_still_works_when_matching() -> None:
    with pytest.warns(DeprecationWarning, match="channel= is deprecated"):
        pgq = PgQueuer(RecorderDriver(), channel=DBSettings().channel)
    assert pgq.channel == DBSettings().channel


def test_in_memory_pgqueuer_threads_settings() -> None:
    settings = DBSettings(prefix="custom_")
    pgq = PgQueuer.in_memory(settings=settings)
    assert isinstance(pgq.queries, InMemoryQueries)
    assert pgq.queries.settings is settings
    assert pgq.channel == settings.channel


def test_channel_defaults_are_not_frozen_at_import_time(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Regression: qm.py/applications.py evaluated DBSettings() at import."""
    monkeypatch.setenv("PGQUEUER_PREFIX", "zz_")
    qm = QueueManager(Queries(RecorderDriver()))
    assert str(qm.channel) == "zz_ch_pgqueuer"
    pgq = PgQueuer(RecorderDriver())
    assert str(pgq.channel) == "zz_ch_pgqueuer"


async def test_completion_watcher_listens_on_the_queries_channel() -> None:
    """Regression: CompletionWatcher hardcoded DBSettings().channel."""
    settings = DBSettings(prefix="custom_")
    driver = RecorderDriver()
    queries = Queries(driver, settings=settings)
    async with CompletionWatcher(driver, queries=queries):
        pass
    assert driver.listened_channels == ["custom_ch_pgqueuer"]


def test_dbsettings_is_frozen() -> None:
    settings = DBSettings()
    with pytest.raises(Exception, match="frozen"):
        settings.prefix = "nope"  # type: ignore[misc]


def test_dbsettings_qualified_is_cached() -> None:
    settings = DBSettings(prefix="custom_")
    assert settings.qualified is settings.qualified
    assert settings.qualified.queue_table == "custom_pgqueuer"


def test_mcp_server_settings_default_is_resolved_at_startup() -> None:
    from pgqueuer.adapters.mcp.server import create_mcp_server

    default = inspect.signature(create_mcp_server).parameters["settings"].default
    assert default is None


async def test_queue_manager_listener_round_trip_under_custom_prefix(
    apgdriver: db.Driver,
) -> None:
    """LISTEN and NOTIFY agree under a custom prefix (fails on the split-channel bug)."""
    await Queries(apgdriver).uninstall()
    settings = DBSettings(prefix="iso_")
    queries = Queries(apgdriver, settings=settings)
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
    queries = Queries(apgdriver, settings=settings)
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

    captured: list[str] = []

    @contextlib.asynccontextmanager
    async def fake_yield_queries(
        ctx: object,
        settings: DBSettings,
    ) -> AsyncGenerator[Queries, None]:
        yield Queries(RecorderDriver(), settings=settings)

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
