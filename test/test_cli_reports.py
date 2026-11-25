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
from pgqueuer.domain import models, types
from test.helpers import env_from_dsn


@pytest.mark.parametrize(
    ("command", "empty_message"),
    [
        ("failed", "No failed jobs."),
        ("stale", "No stale jobs."),
        ("workers", "No workers are holding picked jobs."),
        ("backlog", "No queued jobs."),
        ("size", "No jobs in the queue."),
        ("throughput", "No processed jobs."),
        ("log", "No log entries."),
        ("jobs", "No jobs in the queue."),
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


def test_cli_size_json_counts_per_entrypoint_and_status(dsn: str, pgdriver: db.SyncDriver) -> None:
    q = queries.SyncQueries(pgdriver)
    q.enqueue("a", None)
    q.enqueue("a", None)
    (picked_id,) = q.enqueue("b", None)
    pgdriver.fetch(
        f"UPDATE {DBSettings().queue_table} SET status = 'picked' WHERE id = {picked_id} "
        "RETURNING id"
    )

    result = CliRunner().invoke(app, ["size", "--json"], env=env_from_dsn(dsn))

    assert result.exit_code == 0, result.output
    rows = sorted((r["entrypoint"], r["status"], r["count"]) for r in json.loads(result.stdout))
    assert rows == [("a", "queued", 2), ("b", "picked", 1)]


def test_cli_throughput_json_totals_per_status(dsn: str, pgdriver: db.SyncDriver) -> None:
    env = env_from_dsn(dsn)
    queries.SyncQueries(pgdriver).enqueue("tp", None)

    for period in ([], ["--period", "3600"]):
        result = CliRunner().invoke(app, ["throughput", "--json", *period], env=env)
        assert result.exit_code == 0, result.output
        assert json.loads(result.stdout) == [
            {"entrypoint": "tp", "status": "queued", "total_count": 1}
        ]


def test_cli_log_json_lists_entries_and_filters_exceptions(
    dsn: str, pgdriver: db.SyncDriver
) -> None:
    env = env_from_dsn(dsn)
    (job_id,) = queries.SyncQueries(pgdriver).enqueue("ep", None)
    record = models.TracebackRecord.from_exception(ValueError("boom"), types.JobId(job_id))
    pgdriver.fetch(
        f"INSERT INTO {DBSettings().queue_table_log} "
        "(job_id, status, priority, entrypoint, traceback) VALUES "
        f"({job_id}, 'exception', 0, 'ep', "
        f"'{record.model_dump_json().replace(chr(39), chr(39) * 2)}') RETURNING id"
    )

    every = CliRunner().invoke(app, ["log", "--json"], env=env)
    assert every.exit_code == 0, every.output
    assert sorted(e["status"] for e in json.loads(every.stdout)) == ["exception", "queued"]

    failures = CliRunner().invoke(app, ["log", "--exceptions", "--json"], env=env)
    assert failures.exit_code == 0, failures.output
    (entry,) = json.loads(failures.stdout)
    assert entry["job_id"] == job_id
    assert entry["traceback"]["exception_type"] == "ValueError"

    text = CliRunner().invoke(app, ["log", "--exceptions"], env=env)
    assert text.exit_code == 0, text.output
    assert "ValueError: boom" in text.stdout


def test_cli_jobs_json_pages_and_reports_payload_size(dsn: str, pgdriver: db.SyncDriver) -> None:
    env = env_from_dsn(dsn)
    q = queries.SyncQueries(pgdriver)
    (first,) = q.enqueue("ep", b"\x80\xff\x00")
    (second,) = q.enqueue("ep", None)

    result = CliRunner().invoke(app, ["jobs", "--json", "-n", "1", "--offset", "1"], env=env)
    assert result.exit_code == 0, result.output
    (job,) = json.loads(result.stdout)
    assert job["id"] == second
    assert job["payload_bytes"] == 0

    result = CliRunner().invoke(app, ["jobs", "--json", "-n", "1"], env=env)
    (job,) = json.loads(result.stdout)
    assert job["id"] == first
    assert job["payload_bytes"] == 3
    assert "payload" not in job


def test_cli_tables_json_lists_the_installed_tables(dsn: str) -> None:
    result = CliRunner().invoke(app, ["tables", "--json"], env=env_from_dsn(dsn))

    assert result.exit_code == 0, result.output
    names = {t["table_name"] for t in json.loads(result.stdout)}
    settings = DBSettings()
    assert {settings.queue_table, settings.queue_table_log} <= names


def test_cli_schedules_json(dsn: str, pgdriver: db.SyncDriver) -> None:
    env = env_from_dsn(dsn)
    empty = CliRunner().invoke(app, ["schedules", "--json"], env=env)
    assert empty.exit_code == 0, empty.output
    assert json.loads(empty.stdout) == []

    pgdriver.fetch(
        f"INSERT INTO {DBSettings().schedules_table} (expression, next_run, entrypoint) "
        "VALUES ('* * * * *', NOW(), 'tick') RETURNING id"
    )
    result = CliRunner().invoke(app, ["schedules", "--json"], env=env)
    assert result.exit_code == 0, result.output
    (schedule,) = json.loads(result.stdout)
    assert (schedule["entrypoint"], schedule["expression"]) == ("tick", "* * * * *")


def test_cli_dashboard_json_prints_statistics_once(dsn: str, pgdriver: db.SyncDriver) -> None:
    env = env_from_dsn(dsn)
    queries.SyncQueries(pgdriver).enqueue("ep", None)

    result = CliRunner().invoke(app, ["dashboard", "--json"], env=env)
    assert result.exit_code == 0, result.output
    (row,) = json.loads(result.stdout)
    assert (row["entrypoint"], row["status"], row["count"]) == ("ep", "queued", 1)

    rejected = CliRunner().invoke(app, ["dashboard", "--json", "--interval", "1"], env=env)
    assert rejected.exit_code == 2


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
    [["failed"], ["stale", "--threshold", "0"], ["workers"], ["backlog"], ["jobs"]],
    ids=["failed", "stale", "workers", "backlog", "jobs"],
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
