"""One canonical DBSettings per composition root, honored down the call chain."""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from pgqueuer.adapters.cli.cli import AppConfig
from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.adapters.mcp.server import create_mcp_server
from pgqueuer.adapters.persistence import qb
from pgqueuer.adapters.persistence.queries import Queries, SyncQueries
from pgqueuer.core.applications import PgQueuer
from pgqueuer.core.completion import CompletionWatcher
from pgqueuer.core.qm import QueueManager
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import Channel


class StubSyncDriver:
    def fetch(self, query: str, *args: Any) -> list[dict]:
        raise NotImplementedError


def test_queries_uses_given_settings_object() -> None:
    settings = DBSettings(prefix="acme_")
    q = Queries(InMemoryDriver(), settings=settings)
    assert q.settings is settings
    assert q.qbe.settings is settings
    assert q.qbq.settings is settings
    assert q.qbs.settings is settings


def test_queries_default_builders_share_one_settings() -> None:
    q = Queries(InMemoryDriver())
    assert q.settings is q.qbe.settings
    assert q.qbe.settings is q.qbq.settings
    assert q.qbq.settings is q.qbs.settings


def test_queries_settings_wins_over_injected_builders() -> None:
    settings = DBSettings(prefix="acme_")
    q = Queries(
        InMemoryDriver(),
        qbe=qb.QueryBuilderEnvironment(DBSettings(prefix="other_")),
        settings=settings,
    )
    assert q.qbe.settings is settings
    assert q.qbq.settings is settings
    assert q.qbs.settings is settings


def test_queries_adopts_injected_builder_settings() -> None:
    settings = DBSettings(prefix="acme_")
    q = Queries(
        InMemoryDriver(),
        qbe=qb.QueryBuilderEnvironment(settings),
        qbq=qb.QueryQueueBuilder(settings),
        qbs=qb.QuerySchedulerBuilder(settings),
    )
    assert q.settings is settings


def test_queries_honors_deliberately_divergent_builder() -> None:
    divergent = DBSettings(prefix="other_")
    q = Queries(InMemoryDriver(), qbq=qb.QueryQueueBuilder(divergent))
    assert q.qbq.settings is divergent
    assert q.settings is q.qbe.settings
    assert q.qbs.settings is q.qbe.settings


def test_sync_queries_uses_given_settings_object() -> None:
    settings = DBSettings(prefix="acme_")
    q = SyncQueries(StubSyncDriver(), settings=settings)
    assert q.settings is settings
    assert q.qbq.settings is settings


def test_inmemory_queries_uses_given_settings_object() -> None:
    settings = DBSettings(prefix="acme_")
    q = InMemoryQueries(driver=InMemoryDriver(), settings=settings)
    assert q.settings is settings
    assert q.qbe.settings is settings
    assert q.qbq.settings is settings
    assert q.qbs.settings is settings


def test_queue_manager_channel_derives_from_queries_settings() -> None:
    """Regression: the channel default used to be baked from env at import time."""
    queries = InMemoryQueries(driver=InMemoryDriver(), settings=DBSettings(prefix="acme_"))
    qm = QueueManager(queries)
    assert qm.channel == Channel("acme_ch_pgqueuer")


def test_queue_manager_explicit_channel_wins() -> None:
    queries = InMemoryQueries(driver=InMemoryDriver())
    qm = QueueManager(queries, Channel("custom_channel"))
    assert qm.channel == Channel("custom_channel")


def test_pgqueuer_in_memory_derives_channel_from_settings() -> None:
    pgq = PgQueuer.in_memory(settings=DBSettings(prefix="acme_"))
    assert pgq.channel == Channel("acme_ch_pgqueuer")
    assert pgq.qm.channel == Channel("acme_ch_pgqueuer")


def test_pgqueuer_rejects_conflicting_queries_and_settings() -> None:
    driver = InMemoryDriver()
    queries = InMemoryQueries(driver=driver, settings=DBSettings(prefix="acme_"))
    with pytest.raises(ValueError, match="conflicts"):
        PgQueuer(connection=driver, queries=queries, settings=DBSettings(prefix="other_"))


async def test_completion_watcher_listens_on_given_channel() -> None:
    driver = InMemoryDriver()
    queries = InMemoryQueries(driver=driver, settings=DBSettings(prefix="acme_"))
    async with CompletionWatcher(driver, queries=queries, channel=Channel("acme_ch_pgqueuer")):
        assert "acme_ch_pgqueuer" in driver._listeners


def test_app_config_builds_settings_once_with_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("PGQUEUER_PREFIX", raising=False)
    config = AppConfig(prefix="acme_")
    assert config.settings.queue_table == "acme_pgqueuer"
    assert config.settings.channel == Channel("acme_ch_pgqueuer")


def test_create_mcp_server_settings_default_is_lazy() -> None:
    """Regression: the default used to be a DBSettings() evaluated at import time."""
    assert inspect.signature(create_mcp_server).parameters["settings"].default is None
