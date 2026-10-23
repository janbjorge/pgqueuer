from __future__ import annotations

import dataclasses
from collections.abc import Callable

import pytest
from pydantic import ValidationError

from pgqueuer.adapters.cli.cli import AppConfig
from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.adapters.persistence import qb
from pgqueuer.adapters.persistence.queries import Queries, SyncQueries
from pgqueuer.core.applications import PgQueuer
from pgqueuer.core.completion import CompletionWatcher
from pgqueuer.core.qm import QueueManager
from pgqueuer.domain.settings import (
    ConnectionSettings,
    DBSettings,
    QualifiedNames,
)
from pgqueuer.domain.types import Channel

pytestmark = pytest.mark.usefixtures("clean_connection_env")


def test_statistics_table_status_type_removed() -> None:
    """statistics_table_status_type was removed in v0.27.0."""
    assert "statistics_table_status_type" not in DBSettings.model_fields


def test_prefix_applies_to_all_object_names(monkeypatch: pytest.MonkeyPatch) -> None:
    """PGQUEUER_PREFIX is read at instantiation, not import time."""
    monkeypatch.setenv("PGQUEUER_PREFIX", "acme_")
    settings = DBSettings()
    assert settings.queue_table == "acme_pgqueuer"
    assert settings.channel == "acme_ch_pgqueuer"
    assert settings.function == "acme_fn_pgqueuer_changed"
    assert settings.statistics_table == "acme_pgqueuer_statistics"
    assert settings.queue_status_type == "acme_pgqueuer_status"
    assert settings.queue_table_log == "acme_pgqueuer_log"
    assert settings.trigger == "acme_tg_pgqueuer_changed"
    assert settings.schedules_table == "acme_pgqueuer_schedules"


def test_prefix_skips_explicit_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    """Explicitly set names (constructor or env) are used verbatim, never double-prefixed."""
    assert DBSettings(prefix="acme_", queue_table="jobs").queue_table == "jobs"

    monkeypatch.setenv("PGQUEUER_PREFIX", "acme_")
    monkeypatch.setenv("PGQUEUER_QUEUE_TABLE", "jobs")
    settings = DBSettings()
    assert settings.queue_table == "jobs"
    assert settings.statistics_table == "acme_pgqueuer_statistics"


def test_legacy_bare_env_spelling_still_honored(monkeypatch: pytest.MonkeyPatch) -> None:
    """Pre-refactor env overrides used the bare field name (env_prefix was the prefix value)."""
    monkeypatch.setenv("QUEUE_TABLE", "legacy_jobs")
    assert DBSettings().queue_table == "legacy_jobs"

    monkeypatch.setenv("PGQUEUER_QUEUE_TABLE", "new_jobs")
    assert DBSettings().queue_table == "new_jobs"  # new spelling wins when both are set


def test_invalid_prefix_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGQUEUER_PREFIX", "bad.dot")
    with pytest.raises(ValidationError, match="prefix"):
        DBSettings()


def test_legacy_statistics_status_type_carries_prefix() -> None:
    assert DBSettings().legacy_statistics_status_type == "pgqueuer_statistics_status"
    assert (
        DBSettings(prefix="acme_").legacy_statistics_status_type
        == "acme_pgqueuer_statistics_status"
    )


@pytest.mark.parametrize("bad", ["foo.bar", "foo;drop", "foo bar", "1foo", "foo'"])
def test_invalid_db_schema_rejected(bad: str) -> None:
    with pytest.raises(ValidationError, match="db_schema"):
        DBSettings(db_schema=bad)


def test_db_schema_env_binding(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGQUEUER_SCHEMA", "billing")
    assert DBSettings().db_schema == "billing"


def test_db_schema_lowercase_normalized() -> None:
    """Unquoted identifiers fold to lowercase; the nspname literal must match."""
    settings = DBSettings(db_schema="Billing")
    assert settings.db_schema == "billing"
    assert settings.schema_expr == "'billing'"


def test_object_names_lowercase_normalized(monkeypatch: pytest.MonkeyPatch) -> None:
    """Unquoted identifiers fold to lowercase; the catalog literals must match."""
    settings = DBSettings(prefix="Acme_", statistics_table="AcmeStats")
    assert settings.prefix == "acme_"
    assert settings.queue_table == "acme_pgqueuer"
    assert settings.statistics_table == "acmestats"
    assert settings.qualified.function == "acme_fn_pgqueuer_changed"
    assert settings.trigger == "acme_tg_pgqueuer_changed"
    assert settings.channel == "acme_ch_pgqueuer"

    monkeypatch.setenv("PGQUEUER_PREFIX", "Acme_")
    monkeypatch.setenv("PGQUEUER_QUEUE_TABLE", "AcmeJobs")
    from_env = DBSettings()
    assert (from_env.queue_table, from_env.queue_table_log) == ("acmejobs", "acme_pgqueuer_log")


def test_explicit_channel_keeps_its_case() -> None:
    """A channel is a string literal in pg_notify and quoted in LISTEN, so case is kept."""
    assert DBSettings(channel=Channel("Acme-Events")).channel == "Acme-Events"


def test_qualified_names_with_schema() -> None:
    settings = DBSettings(db_schema="billing", prefix="acme_")
    assert settings.qualified.queue_table == "billing.acme_pgqueuer"
    assert settings.qualified.function == "billing.acme_fn_pgqueuer_changed"
    assert settings.qualified.queue_status_type == "billing.acme_pgqueuer_status"
    assert settings.qualify("anything") == "billing.anything"


def test_qualified_names_without_schema_are_bare() -> None:
    settings = DBSettings()
    assert settings.qualified.queue_table == settings.queue_table
    assert settings.schema_expr == "current_schema()"
    assert settings.qualify("anything") == "anything"


def test_qualified_is_cached() -> None:
    settings = DBSettings(db_schema="billing")
    assert settings.qualified is settings.qualified


def test_dotted_name_conflicts_with_db_schema() -> None:
    """Dotted names stay allowed standalone (legacy DIY hack) but not with db_schema."""
    assert DBSettings(queue_table="myschema.pgqueuer").queue_table == "myschema.pgqueuer"
    with pytest.raises(ValidationError, match="schema qualifier"):
        DBSettings(queue_table="myschema.pgqueuer", db_schema="other")


def test_qualified_names_omit_channel_and_trigger() -> None:
    """Channel (database-global) and trigger (per-table) must never be schema-qualified."""
    fields = {f.name for f in dataclasses.fields(QualifiedNames)}
    assert fields == {
        "queue_table",
        "queue_table_log",
        "statistics_table",
        "schedules_table",
        "function",
        "queue_status_type",
    }


def test_connection_settings_defaults() -> None:
    settings = ConnectionSettings()
    assert settings.dsn is None
    assert settings.pool_min_size == 1
    assert settings.pool_max_size == 5
    assert settings.connect_timeout is None
    assert settings.application_name is None


def test_connection_settings_reads_pgqueuer_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGQUEUER_POOL_MIN_SIZE", "2")
    monkeypatch.setenv("PGQUEUER_POOL_MAX_SIZE", "9")
    monkeypatch.setenv("PGQUEUER_CONNECT_TIMEOUT", "4.5")
    monkeypatch.setenv("PGQUEUER_APPLICATION_NAME", "pgq-env")
    settings = ConnectionSettings()
    assert settings.pool_min_size == 2
    assert settings.pool_max_size == 9
    assert settings.connect_timeout == 4.5
    assert settings.application_name == "pgq-env"


def test_connection_settings_accepts_dsn_by_field_name() -> None:
    # Requires populate_by_name=True: without it the init kwarg would be
    # swallowed by extra="ignore" because the field has a validation_alias.
    assert ConnectionSettings(dsn="postgresql://x/y").dsn == "postgresql://x/y"


def test_connection_settings_dsn_from_pgdsn(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGDSN", "postgresql://example/db")
    assert ConnectionSettings().dsn == "postgresql://example/db"


def test_connection_settings_pgqueuer_dsn_wins_over_pgdsn(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PGDSN", "postgresql://example/libpq")
    monkeypatch.setenv("PGQUEUER_DSN", "postgresql://example/pgqueuer")
    assert ConnectionSettings().dsn == "postgresql://example/pgqueuer"


@pytest.mark.parametrize("make", [Queries, InMemoryQueries])
def test_queries_builders_share_one_settings(
    make: Callable[[InMemoryDriver], Queries | InMemoryQueries],
) -> None:
    q = make(InMemoryDriver())
    assert q.qbe.settings is q.qbq.settings is q.qbs.settings is q.settings


@pytest.mark.parametrize("make", [Queries, InMemoryQueries])
def test_queries_builders_share_passed_settings(
    make: Callable[..., Queries | InMemoryQueries],
) -> None:
    settings = DBSettings(prefix="billing_")
    q = make(InMemoryDriver(), settings=settings)
    assert q.settings is q.qbe.settings is q.qbq.settings is q.qbs.settings is settings


def test_sync_queries_uses_passed_settings() -> None:
    settings = DBSettings(prefix="billing_")
    q = SyncQueries(InMemoryDriver(), settings)  # type: ignore[arg-type]
    assert q.qbq.settings is settings


@pytest.mark.parametrize("make", [Queries, InMemoryQueries])
def test_builders_are_not_constructor_arguments(make: Callable[..., object]) -> None:
    """Separate builders could disagree on names; settings= is the one input."""
    with pytest.raises(TypeError, match="unexpected keyword argument"):
        make(InMemoryDriver(), qbe=qb.QueryBuilderEnvironment(DBSettings()))


def test_passed_settings_set_the_channel() -> None:
    """QueueManager and PgQueuer listen where their queries notify."""
    settings = DBSettings(prefix="billing_")
    driver = InMemoryDriver()
    assert QueueManager(InMemoryQueries(driver, settings)).channel == "billing_ch_pgqueuer"
    assert PgQueuer(driver, settings=settings).channel == "billing_ch_pgqueuer"
    assert PgQueuer.in_memory(settings=settings).channel == "billing_ch_pgqueuer"
    assert PgQueuer(driver, Channel("explicit"), settings=settings).channel == "explicit"


def test_pgqueuer_rejects_settings_beside_other_queries() -> None:
    driver = InMemoryDriver()
    with pytest.raises(ValueError, match="Pass settings to the queries"):
        PgQueuer(driver, queries=InMemoryQueries(driver), settings=DBSettings(prefix="x_"))


async def test_completion_watcher_listens_on_the_queries_channel() -> None:
    """#938 repro: a prefixed Queries notifies on its channel, so the watcher must listen there."""
    driver = InMemoryDriver()
    queries = InMemoryQueries(driver, DBSettings(prefix="billing_"))
    async with CompletionWatcher(driver, queries=queries, refresh_interval=None):
        assert driver._listeners["billing_ch_pgqueuer"]
        assert not driver._listeners["ch_pgqueuer"]


def test_channel_default_resolves_at_construction(monkeypatch: pytest.MonkeyPatch) -> None:
    """The default channel follows PGQUEUER_PREFIX set after pgqueuer was imported."""
    monkeypatch.setenv("PGQUEUER_PREFIX", "acme_")
    driver = InMemoryDriver()
    assert QueueManager(InMemoryQueries(driver=driver)).channel == "acme_ch_pgqueuer"
    assert PgQueuer(driver).channel == "acme_ch_pgqueuer"


def test_cli_settings_follow_prefix_and_schema() -> None:
    config = AppConfig(prefix="acme_", schema="billing")
    settings = config.settings
    assert config.settings is settings
    assert (settings.prefix, settings.db_schema) == ("acme_", "billing")
    assert settings.queue_table == "acme_pgqueuer"


@pytest.mark.parametrize(
    "make",
    [
        lambda: PgQueuer(InMemoryDriver()),
        lambda: PgQueuer(InMemoryDriver(), settings=DBSettings(prefix="billing_")),
        lambda: PgQueuer(InMemoryDriver(), queries=InMemoryQueries(InMemoryDriver())),
        PgQueuer.in_memory,
        lambda: PgQueuer.in_memory(settings=DBSettings(prefix="billing_")),
    ],
)
def test_managers_share_one_queries(make: Callable[[], PgQueuer]) -> None:
    """Both managers hold the same queries, so they read the same settings."""
    pgq = make()
    assert pgq.qm.queries is pgq.sm.queries is pgq.queries


# Before #945 only prefix and db_schema were validated, so a name like
# "evil; CREATE TABLE x; --" reached the SQL string verbatim. Object names must
# be identifiers or dotted identifier chains; the channel, which goes into SQL
# as a string literal, may also hold dots and dashes.

OBJECT_NAME_FIELDS = [
    "function",
    "statistics_table",
    "queue_status_type",
    "queue_table",
    "queue_table_log",
    "trigger",
    "schedules_table",
]


@pytest.mark.parametrize("field", [*OBJECT_NAME_FIELDS, "channel"])
@pytest.mark.parametrize("bad", ["evil; CREATE TABLE x; --", "x'y", "x y", 'x"y'])
def test_name_field_rejects_sql(field: str, bad: str, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(f"PGQUEUER_{field.upper()}", bad)
    with pytest.raises(ValidationError):
        DBSettings()


@pytest.mark.parametrize("field", OBJECT_NAME_FIELDS)
@pytest.mark.parametrize("bad", ["x-y", "x..y", ".x", "x."])
def test_object_name_rejects_non_identifier(
    field: str, bad: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv(f"PGQUEUER_{field.upper()}", bad)
    with pytest.raises(ValidationError):
        DBSettings()


def test_default_names_are_valid() -> None:
    DBSettings()
    DBSettings(prefix="acme_")


def test_dotted_object_name_still_accepted() -> None:
    assert DBSettings(queue_table="myschema.pgqueuer").queue_table == "myschema.pgqueuer"


def test_channel_keeps_dots_and_dashes() -> None:
    assert DBSettings(channel=Channel("my-app.events")).channel == "my-app.events"
