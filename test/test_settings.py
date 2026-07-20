from __future__ import annotations

import dataclasses

import pytest
from pydantic import ValidationError

from pgqueuer.domain.settings import (
    ConnectionSettings,
    DBSettings,
    QualifiedNames,
    add_prefix,
)

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


def test_add_prefix_shim_warns_and_concats(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PGQUEUER_PREFIX", "acme_")
    with pytest.deprecated_call():
        assert add_prefix("pgqueuer") == "acme_pgqueuer"


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
