from __future__ import annotations

import json
import re
import time
import uuid
from collections.abc import AsyncGenerator, Iterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

import psycopg
import pytest
from typer.testing import CliRunner

from pgqueuer import db, queries
from pgqueuer.adapters.cli.cli import app
from pgqueuer.adapters.persistence.qb import DBSettings
from test.helpers import env_from_dsn


@pytest.mark.parametrize(
    ("command", "empty_message"),
    [
        ("failed", "No failed jobs."),
        ("stale", "No stale jobs."),
        ("workers", "No workers are holding picked jobs."),
        ("backlog", "No queued jobs."),
    ],
)
def test_cli_report_empty(dsn: str, command: str, empty_message: str) -> None:
    runner = CliRunner()
    env = env_from_dsn(dsn)

    as_json = runner.invoke(app, [command, "--json"], env=env)
    assert as_json.exit_code == 0, as_json.output
    assert json.loads(as_json.stdout) == []

    as_text = runner.invoke(app, [command], env=env)
    assert as_text.exit_code == 0, as_text.output
    assert as_text.stdout.strip() == empty_message


def test_cli_failed_json_reports_payload_size(dsn: str, pgdriver: db.SyncDriver) -> None:
    env = env_from_dsn(dsn)
    (job_id,) = queries.SyncQueries(pgdriver).enqueue("ep", b"\x80\xff\x00")
    pgdriver.fetch(f"UPDATE {DBSettings().queue_table} SET status = 'failed' RETURNING id")

    result = CliRunner().invoke(app, ["failed", "--json"], env=env)

    assert result.exit_code == 0, result.output
    (job,) = json.loads(result.stdout)
    assert job["id"] == job_id
    assert job["entrypoint"] == "ep"
    assert job["status"] == "failed"
    assert job["payload_bytes"] == 3
    assert "payload" not in job


def test_cli_stale_json_reports_old_heartbeat(dsn: str, pgdriver: db.SyncDriver) -> None:
    env = env_from_dsn(dsn)
    q = queries.SyncQueries(pgdriver)
    (stale_id,) = q.enqueue("ep", None)
    q.enqueue("ep", None)
    table = DBSettings().queue_table
    pgdriver.fetch(f"UPDATE {table} SET status = 'picked' RETURNING id")
    pgdriver.fetch(
        f"UPDATE {table} SET heartbeat = NOW() - interval '1 hour' "
        f"WHERE id = {stale_id} RETURNING id"
    )

    result = CliRunner().invoke(app, ["stale", "--json", "--threshold", "60"], env=env)

    assert result.exit_code == 0, result.output
    (job,) = json.loads(result.stdout)
    assert job["id"] == stale_id
    assert job["status"] == "picked"
    assert job["seconds_since_heartbeat"] >= 3600


def test_cli_workers_json_groups_picked_jobs(dsn: str, pgdriver: db.SyncDriver) -> None:
    env = env_from_dsn(dsn)
    q = queries.SyncQueries(pgdriver)
    q.enqueue("a", None)
    q.enqueue("b", None)
    q.enqueue("c", None)
    qm_id = uuid.uuid4()
    pgdriver.fetch(
        f"UPDATE {DBSettings().queue_table} SET status = 'picked', "
        f"queue_manager_id = '{qm_id}' WHERE entrypoint IN ('a', 'b') RETURNING id"
    )

    result = CliRunner().invoke(app, ["workers", "--json"], env=env)

    assert result.exit_code == 0, result.output
    (worker,) = json.loads(result.stdout)
    assert worker["queue_manager_id"] == str(qm_id)
    assert worker["active_jobs"] == 2
    assert sorted(worker["entrypoints"]) == ["a", "b"]


def test_cli_backlog_json_counts_queued_per_entrypoint(
    dsn: str,
    pgdriver: db.SyncDriver,
) -> None:
    env = env_from_dsn(dsn)
    q = queries.SyncQueries(pgdriver)
    q.enqueue("a", None)
    q.enqueue("a", None)
    (picked_id,) = q.enqueue("b", None)
    table = DBSettings().queue_table
    pgdriver.fetch(f"UPDATE {table} SET created = NOW() - interval '1 hour' RETURNING id")
    pgdriver.fetch(f"UPDATE {table} SET status = 'picked' WHERE id = {picked_id} RETURNING id")

    result = CliRunner().invoke(app, ["backlog", "--json"], env=env)

    assert result.exit_code == 0, result.output
    (row,) = json.loads(result.stdout)
    assert row["entrypoint"] == "a"
    assert row["queued_count"] == 2
    assert row["oldest_age_seconds"] >= 3600


@asynccontextmanager
async def psycopg_queries() -> AsyncGenerator[queries.Queries, None]:
    """``--factory`` target: the CLI on psycopg, connected from the PG* env vars."""
    async with await psycopg.AsyncConnection.connect(autocommit=True) as conn:
        yield queries.Queries(db.PsycopgDriver(conn))


@pytest.fixture
def tokyo_terminal(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Run the CLI as if the terminal's local zone were Asia/Tokyo (+09:00, no DST)."""
    if not hasattr(time, "tzset"):
        pytest.skip("time.tzset is POSIX only")
    monkeypatch.setenv("TZ", "Asia/Tokyo")
    time.tzset()
    yield
    monkeypatch.undo()
    time.tzset()


def seed_report_rows(pgdriver: db.SyncDriver) -> None:
    """One held, one picked and one queued job, every timestamp 2026-01-02 03:04:05 UTC."""
    q = queries.SyncQueries(pgdriver)
    (held_id,) = q.enqueue("held", None)
    (picked_id,) = q.enqueue("busy", None)
    q.enqueue("waiting", None)
    table = DBSettings().queue_table
    pgdriver.fetch(
        f"UPDATE {table} SET created = '2026-01-02 03:04:05+00', "
        f"heartbeat = '2026-01-02 03:04:05+00' RETURNING id"
    )
    pgdriver.fetch(f"UPDATE {table} SET status = 'failed' WHERE id = {held_id} RETURNING id")
    pgdriver.fetch(
        f"UPDATE {table} SET status = 'picked', queue_manager_id = '{uuid.uuid4()}' "
        f"WHERE id = {picked_id} RETURNING id"
    )


@pytest.mark.parametrize(
    "command",
    [["failed"], ["stale", "--threshold", "0"], ["workers"], ["backlog"]],
    ids=["failed", "stale", "workers", "backlog"],
)
@pytest.mark.parametrize(
    "factory",
    [[], ["--factory", "test.test_cli_reports:psycopg_queries"]],
    ids=["asyncpg", "psycopg"],
)
@pytest.mark.usefixtures("tokyo_terminal")
def test_cli_report_prints_times_in_the_terminal_zone(
    dsn: str,
    pgdriver: db.SyncDriver,
    command: list[str],
    factory: list[str],
) -> None:
    """Text reports show local time with its offset, whichever driver read the row."""
    seed_report_rows(pgdriver)
    # A session zone that is neither UTC nor the terminal's: psycopg returns times in it.
    env = env_from_dsn(dsn) | {"PGTZ": "America/New_York"}

    result = CliRunner().invoke(app, [*factory, *command], env=env)

    assert result.exit_code == 0, result.output
    assert "2026-01-02 12:04:05+09:00" in result.stdout
    # No column may fall back to a bare wall-clock reading (UTC or session zone).
    assert not re.search(r"\d{4}-\d\d-\d\d \d\d:\d\d:\d\d(?![+-])", result.stdout)


@pytest.mark.parametrize(
    ("command", "field"),
    [
        (["failed"], "created"),
        (["stale", "--threshold", "0"], "heartbeat"),
        (["workers"], "oldest_heartbeat"),
        (["backlog"], "oldest_created"),
    ],
    ids=["failed", "stale", "workers", "backlog"],
)
@pytest.mark.parametrize(
    "factory",
    [[], ["--factory", "test.test_cli_reports:psycopg_queries"]],
    ids=["asyncpg", "psycopg"],
)
@pytest.mark.usefixtures("tokyo_terminal")
def test_cli_report_json_keeps_the_instant(
    dsn: str,
    pgdriver: db.SyncDriver,
    command: list[str],
    field: str,
    factory: list[str],
) -> None:
    """--json carries the same instant on both drivers, so only the text rendering differed."""
    seed_report_rows(pgdriver)
    env = env_from_dsn(dsn) | {"PGTZ": "America/New_York"}

    result = CliRunner().invoke(app, [*factory, *command, "--json"], env=env)

    assert result.exit_code == 0, result.output
    (row,) = json.loads(result.stdout)
    value = datetime.fromisoformat(row[field])
    assert value == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
    # asyncpg hands back UTC; psycopg hands back the session zone PGTZ asked for (EST).
    assert value.utcoffset() == (timedelta(hours=-5) if factory else timedelta(0))
