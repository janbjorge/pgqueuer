from __future__ import annotations

import pytest

from pgqueuer import db
from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.adapters.persistence import qb
from pgqueuer.adapters.persistence.queries import Queries, SyncQueries
from pgqueuer.applications import PgQueuer
from pgqueuer.core.completion import CompletionWatcher
from pgqueuer.core.qm import QueueManager
from pgqueuer.domain.models import Channel
from pgqueuer.domain.settings import DBSettings


def test_queries_shares_one_settings_object() -> None:
    settings = DBSettings(prefix="acme_")
    q = Queries(InMemoryDriver(), settings=settings)

    assert q.settings is settings
    assert q.qbe.settings is settings
    assert q.qbq.settings is settings
    assert q.qbs.settings is settings


def test_queries_legacy_builder_injection_uses_builder_settings() -> None:
    settings = DBSettings(prefix="legacy_")
    q = Queries(
        InMemoryDriver(),
        qbe=qb.QueryBuilderEnvironment(settings=settings),
    )

    assert q.settings is settings
    assert q.qbq.settings is settings
    assert q.qbs.settings is settings


def test_inmemory_queries_shares_one_settings_object() -> None:
    settings = DBSettings(prefix="mem_")
    q = InMemoryQueries(InMemoryDriver(), settings=settings)

    assert q.settings is settings
    assert q.qbe.settings is settings
    assert q.qbq.settings is settings
    assert q.qbs.settings is settings


def test_sync_queries_shares_one_settings_object(pgdriver: db.SyncDriver) -> None:
    settings = DBSettings(prefix="sync_")
    q = SyncQueries(pgdriver, settings=settings)

    assert q.settings is settings
    assert q.qbq.settings is settings


def test_settings_mutation_updates_qualified_sql() -> None:
    settings = DBSettings()
    q = Queries(InMemoryDriver(), settings=settings)
    before = q.qbq.build_dequeue_query()

    settings.db_schema = "billing"
    after = q.qbq.build_dequeue_query()

    assert before != after
    assert "billing." in after


def test_queue_manager_channel_override_updates_repository_settings() -> None:
    settings = DBSettings()
    queries = Queries(InMemoryDriver(), settings=settings)
    custom = Channel("custom_notify")

    qm = QueueManager(queries, channel=custom)

    assert qm.channel == custom
    assert queries.settings.channel == custom


def test_queue_manager_defaults_channel_from_repository_settings() -> None:
    settings = DBSettings(prefix="qm_")
    queries = Queries(InMemoryDriver(), settings=settings)

    qm = QueueManager(queries)

    assert qm.channel == settings.channel


def test_pgqueuer_channel_override_updates_queries_settings() -> None:
    custom = Channel("edge_channel")
    pgq = PgQueuer.in_memory(channel=custom)

    assert pgq.channel == custom
    assert pgq.queries is not None
    assert pgq.queries.settings.channel == custom


async def test_completion_watcher_listens_on_repository_channel() -> None:
    settings = DBSettings(prefix="watch_")
    driver = InMemoryDriver()
    queries = Queries(driver, settings=settings)
    watcher = CompletionWatcher(driver, queries=queries)

    async with watcher:
        assert driver._listeners[settings.channel]


@pytest.mark.parametrize("invalid_schema", ["bad.schema", "bad;drop"])
def test_settings_assignment_validates(invalid_schema: str) -> None:
    settings = DBSettings()
    with pytest.raises(ValueError, match="db_schema"):
        settings.db_schema = invalid_schema
