from __future__ import annotations

import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
import typer
from typer.testing import CliRunner

from pgqueuer import Job, PgQueuer, db, queries
from pgqueuer.adapters.cli import cli, supervisor
from pgqueuer.adapters.connections import connect_asyncpg, connect_psycopg
from pgqueuer.core import logconfig
from pgqueuer.domain import types
from pgqueuer.domain.settings import ConnectionSettings


@pytest.mark.parametrize("flag", [[], ["--log-level", "debug"], ["--log-level", "WARNING"]])
def test_run_passes_enums_not_strings(monkeypatch: pytest.MonkeyPatch, flag: list[str]) -> None:
    """Regression for #1097: --log-level reached setup_fancy_logger as a str."""
    seen: dict[str, Any] = {}

    async def fake_run(_factory: object, **kwargs: Any) -> None:
        seen.update(kwargs)

    monkeypatch.setattr(logconfig, "setup_fancy_logger", lambda level: seen.update(level=level))
    monkeypatch.setattr(supervisor, "run", fake_run)

    result = CliRunner().invoke(cli.app, ["run", "examples.consumer:main", *flag])

    assert result.exit_code == 0, result.output
    assert isinstance(seen["level"], logconfig.LogLevel)
    assert isinstance(seen["mode"], types.QueueExecutionMode)


def test_pg_dsn_option_does_not_read_pgdsn(monkeypatch: pytest.MonkeyPatch) -> None:
    """The --pg-dsn option does not read PGDSN (#1126)."""
    monkeypatch.setenv("PGDSN", "postgresql://example/libpq")
    ctx = typer.main.get_command(cli.app).make_context("pgq", ["install"])
    assert not ctx.params["pg_dsn"]


# The factories run inside the CLI, so they report to the tests through these lists.
seen_dsns: list[dict[str, str | None]] = []
ran: list[int] = []


def note_dsn() -> None:
    seen_dsns.append(
        {
            "PGQUEUER_DSN": os.environ.get("PGQUEUER_DSN"),
            "ConnectionSettings().dsn": ConnectionSettings().dsn,
        }
    )


def record_runs(pgq: PgQueuer) -> PgQueuer:
    @pgq.entrypoint("ep")
    async def ep(job: Job) -> None:
        ran.append(job.id)

    return pgq


@asynccontextmanager
async def asyncpg_worker() -> AsyncGenerator[PgQueuer, None]:
    """``pgq run`` target: connects with no DSN argument, as a user factory does."""
    note_dsn()
    async with connect_asyncpg() as connection:
        yield record_runs(PgQueuer.from_asyncpg_connection(connection))


@asynccontextmanager
async def psycopg_worker() -> AsyncGenerator[PgQueuer, None]:
    """``pgq run`` target: connects with no DSN argument, as a user factory does."""
    note_dsn()
    async with connect_psycopg() as connection:
        yield record_runs(PgQueuer.from_psycopg_connection(connection))


@asynccontextmanager
async def asyncpg_queries() -> AsyncGenerator[queries.Queries, None]:
    """``--factory`` target: connects with no DSN argument, as a user factory does."""
    note_dsn()
    async with connect_asyncpg() as connection:
        yield queries.Queries(db.AsyncpgDriver(connection))


@pytest.fixture
def no_fallback_server(
    clean_connection_env: None, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Remove every database a factory can reach without the DSN from the CLI.

    PGHOST names an empty directory, so a connect that falls back to libpq fails at once.
    """
    monkeypatch.setenv("PGHOST", str(tmp_path))
    seen_dsns.clear()
    ran.clear()


@pytest.mark.usefixtures("no_fallback_server")
@pytest.mark.parametrize("factory", ["asyncpg_worker", "psycopg_worker"])
def test_run_hands_pg_dsn_to_the_factory(dsn: str, pgdriver: db.SyncDriver, factory: str) -> None:
    """pgq run gives --pg-dsn to the factory through PGQUEUER_DSN."""
    (job_id,) = queries.SyncQueries(pgdriver).enqueue("ep", None)

    result = CliRunner().invoke(
        cli.app, ["--pg-dsn", dsn, "run", f"{__name__}:{factory}", "--mode", "drain"]
    )

    assert seen_dsns == [{"PGQUEUER_DSN": dsn, "ConnectionSettings().dsn": dsn}]
    assert result.exit_code == 0, result.output
    assert ran == [job_id]


@pytest.mark.usefixtures("no_fallback_server")
def test_run_without_pg_dsn_leaves_pgdsn_to_the_factory(
    dsn: str, pgdriver: db.SyncDriver, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without --pg-dsn, the factory still connects through PGDSN."""
    monkeypatch.setenv("PGDSN", dsn)
    (job_id,) = queries.SyncQueries(pgdriver).enqueue("ep", None)

    result = CliRunner().invoke(cli.app, ["run", f"{__name__}:asyncpg_worker", "--mode", "drain"])

    assert seen_dsns == [{"PGQUEUER_DSN": None, "ConnectionSettings().dsn": dsn}]
    assert result.exit_code == 0, result.output
    assert ran == [job_id]


@pytest.mark.usefixtures("no_fallback_server")
def test_factory_option_hands_pg_dsn_to_the_factory(dsn: str, pgdriver: db.SyncDriver) -> None:
    """A --factory factory gets --pg-dsn through PGQUEUER_DSN."""
    result = CliRunner().invoke(
        cli.app, ["--pg-dsn", dsn, "--factory", f"{__name__}:asyncpg_queries", "queue", "ep"]
    )

    assert seen_dsns == [{"PGQUEUER_DSN": dsn, "ConnectionSettings().dsn": dsn}]
    assert result.exit_code == 0, result.output
    (row,) = queries.SyncQueries(pgdriver).queue_size()
    assert (row.entrypoint, row.status, row.count) == ("ep", "queued", 1)
