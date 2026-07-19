"""Tests for the shared connection/pool factories (issue #701).

The load-bearing property: kwargs the user did not explicitly set must be
absent (not None), so DSN parameters and libpq env vars keep full control.
"""

from __future__ import annotations

import pytest

from pgqueuer.adapters import connections
from pgqueuer.domain.settings import ConnectionSettings

CONNECTION_ENV_VARS = (
    "PGQUEUER_DSN",
    "PGDSN",
    "PGQUEUER_POOL_MIN_SIZE",
    "PGQUEUER_POOL_MAX_SIZE",
    "PGQUEUER_CONNECT_TIMEOUT",
    "PGQUEUER_APPLICATION_NAME",
    "PGCONNECT_TIMEOUT",
)


@pytest.fixture(autouse=True)
def clean_connection_env(monkeypatch: pytest.MonkeyPatch) -> None:
    for var in CONNECTION_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


def with_query_param(dsn: str, param: str) -> str:
    return f"{dsn}{'&' if '?' in dsn else '?'}{param}"


class TestAsyncpgConnectKwargs:
    def test_unset_optionals_produce_no_kwargs(self) -> None:
        assert connections._asyncpg_connect_kwargs(ConnectionSettings()) == {}

    def test_connect_timeout_from_settings(self) -> None:
        kwargs = connections._asyncpg_connect_kwargs(ConnectionSettings(connect_timeout=2.5))
        assert kwargs == {"timeout": 2.5}

    def test_pgconnect_timeout_env_fallback(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("PGCONNECT_TIMEOUT", "7")
        kwargs = connections._asyncpg_connect_kwargs(ConnectionSettings())
        assert kwargs == {"timeout": 7.0}

    def test_pgqueuer_timeout_wins_over_pgconnect_timeout(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setenv("PGCONNECT_TIMEOUT", "7")
        monkeypatch.setenv("PGQUEUER_CONNECT_TIMEOUT", "3")
        kwargs = connections._asyncpg_connect_kwargs(ConnectionSettings())
        assert kwargs == {"timeout": 3.0}

    def test_application_name_maps_to_server_settings(self) -> None:
        kwargs = connections._asyncpg_connect_kwargs(
            ConnectionSettings(application_name="pgq-test")
        )
        assert kwargs == {"server_settings": {"application_name": "pgq-test"}}


class TestPsycopgConnectKwargs:
    def test_default_is_autocommit_only(self) -> None:
        assert connections._psycopg_connect_kwargs(ConnectionSettings()) == {"autocommit": True}

    def test_pgconnect_timeout_env_not_forwarded(self, monkeypatch: pytest.MonkeyPatch) -> None:
        # libpq reads PGCONNECT_TIMEOUT natively with correct precedence;
        # forwarding it here would invert DSN-over-env precedence.
        monkeypatch.setenv("PGCONNECT_TIMEOUT", "7")
        assert connections._psycopg_connect_kwargs(ConnectionSettings()) == {"autocommit": True}

    def test_connect_timeout_rounded_up_to_int(self) -> None:
        kwargs = connections._psycopg_connect_kwargs(ConnectionSettings(connect_timeout=1.5))
        assert kwargs == {"autocommit": True, "connect_timeout": 2}

    def test_application_name_forwarded(self) -> None:
        kwargs = connections._psycopg_connect_kwargs(ConnectionSettings(application_name="pgq"))
        assert kwargs == {"autocommit": True, "application_name": "pgq"}


class TestCreateAsyncpgPoolForwarding:
    async def test_sizes_forwarded_and_dsn_verbatim(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import asyncpg

        captured: dict[str, object] = {}

        class FakePool:
            async def __aenter__(self) -> FakePool:
                return self

            async def __aexit__(self, *exc: object) -> None:
                return None

        def fake_create_pool(**kwargs: object) -> FakePool:
            captured.update(kwargs)
            return FakePool()

        monkeypatch.setattr(asyncpg, "create_pool", fake_create_pool)

        weird_dsn = "postgresql://u@h:5432/db?application_name=weird&target_session_attrs=primary"
        async with connections.create_asyncpg_pool(
            dsn=weird_dsn,
            settings=ConnectionSettings(pool_min_size=2, pool_max_size=7),
        ):
            pass

        assert captured["dsn"] == weird_dsn
        assert captured["min_size"] == 2
        assert captured["max_size"] == 7
        assert "timeout" not in captured
        assert "server_settings" not in captured


class TestCliDefaultFactory:
    async def test_forwards_connection_settings_from_env(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        import asyncpg

        from pgqueuer.adapters.cli.cli import AppConfig, create_default_queries_factory
        from pgqueuer.adapters.persistence.qb import DBSettings

        captured: dict[str, object] = {}

        async def fake_connect(**kwargs: object) -> object:
            captured.update(kwargs)
            return object()  # AsyncpgDriver only stores the connection at init.

        monkeypatch.setattr(asyncpg, "connect", fake_connect)
        monkeypatch.setenv("PGQUEUER_APPLICATION_NAME", "pgq-cli")

        factory = create_default_queries_factory(AppConfig(), DBSettings())
        async with factory():
            pass

        assert captured["dsn"] is None
        assert captured["server_settings"] == {"application_name": "pgq-cli"}

    async def test_no_extra_kwargs_when_env_unset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        import asyncpg

        from pgqueuer.adapters.cli.cli import AppConfig, create_default_queries_factory
        from pgqueuer.adapters.persistence.qb import DBSettings

        captured: dict[str, object] = {}

        async def fake_connect(**kwargs: object) -> object:
            captured.update(kwargs)
            return object()

        monkeypatch.setattr(asyncpg, "connect", fake_connect)

        factory = create_default_queries_factory(AppConfig(pg_dsn="postgresql://x/y"), DBSettings())
        async with factory():
            pass

        assert captured == {"dsn": "postgresql://x/y"}


class TestAsyncpgPoolIntegration:
    async def test_pool_executes_query(self, dsn: str) -> None:
        async with connections.create_asyncpg_pool(dsn=dsn) as pool:
            assert await pool.fetchval("SELECT 1") == 1

    async def test_application_name_applied_when_set(self, dsn: str) -> None:
        settings = ConnectionSettings(application_name="pgq-conn-test")
        async with connections.create_asyncpg_pool(dsn=dsn, settings=settings) as pool:
            current = await pool.fetchval("SELECT current_setting('application_name')")
        assert current == "pgq-conn-test"

    async def test_dsn_application_name_not_clobbered(self, dsn: str) -> None:
        async with connections.create_asyncpg_pool(
            dsn=with_query_param(dsn, "application_name=weird")
        ) as pool:
            current = await pool.fetchval("SELECT current_setting('application_name')")
        assert current == "weird"

    async def test_single_connection_via_connect_asyncpg(self, dsn: str) -> None:
        connection = await connections.connect_asyncpg(dsn=dsn)
        try:
            assert await connection.fetchval("SELECT 1") == 1
        finally:
            await connection.close()


class TestPsycopgPoolIntegration:
    async def test_pool_executes_query_with_autocommit(self, dsn: str) -> None:
        async with (
            connections.create_psycopg_pool(dsn=dsn) as pool,
            pool.connection() as conn,
        ):
            assert conn.autocommit
            cursor = await conn.execute("SELECT 1")
            row = await cursor.fetchone()
            assert row is not None and row[0] == 1

    async def test_application_name_applied_when_set(self, dsn: str) -> None:
        settings = ConnectionSettings(application_name="pgq-psycopg-test")
        async with (
            connections.create_psycopg_pool(dsn=dsn, settings=settings) as pool,
            pool.connection() as conn,
        ):
            cursor = await conn.execute("SELECT current_setting('application_name')")
            row = await cursor.fetchone()
        assert row is not None and row[0] == "pgq-psycopg-test"

    async def test_dsn_application_name_not_clobbered(self, dsn: str) -> None:
        async with (
            connections.create_psycopg_pool(
                dsn=with_query_param(dsn, "application_name=weird")
            ) as pool,
            pool.connection() as conn,
        ):
            cursor = await conn.execute("SELECT current_setting('application_name')")
            row = await cursor.fetchone()
        assert row is not None and row[0] == "weird"
