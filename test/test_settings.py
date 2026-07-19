from __future__ import annotations

import pytest

from pgqueuer.domain.settings import ConnectionSettings

pytestmark = pytest.mark.usefixtures("clean_connection_env")


def test_statistics_table_status_type_removed() -> None:
    """statistics_table_status_type was removed in v0.27.0."""
    from pgqueuer.domain.settings import DBSettings

    assert "statistics_table_status_type" not in DBSettings.model_fields


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
