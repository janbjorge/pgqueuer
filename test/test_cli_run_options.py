from __future__ import annotations

from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pytest
import typer
from typer.testing import CliRunner

from pgqueuer import PgQueuer
from pgqueuer.adapters.cli import cli, supervisor
from pgqueuer.adapters.connections import connect_asyncpg
from pgqueuer.core import logconfig
from pgqueuer.domain import types


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


@asynccontextmanager
async def factory_without_dsn() -> AsyncGenerator[PgQueuer, None]:
    async with connect_asyncpg() as connection:
        yield PgQueuer.from_asyncpg_connection(connection)


@pytest.mark.usefixtures("clean_connection_env")
def test_run_hands_pg_dsn_to_the_factory(
    dsn: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """A factory that connects with no DSN uses the --pg-dsn value."""
    # PGHOST names an empty directory, so only --pg-dsn can reach the database.
    monkeypatch.setenv("PGHOST", str(tmp_path))
    result = CliRunner().invoke(
        cli.app, ["--pg-dsn", dsn, "run", f"{__name__}:factory_without_dsn", "--mode", "drain"]
    )
    assert result.exception is None, result.output
