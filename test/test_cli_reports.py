from __future__ import annotations

import json
import uuid

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
