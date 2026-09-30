from __future__ import annotations

import json
import os

from typer.testing import CliRunner

from pgqueuer import db, queries
from pgqueuer.adapters.cli.cli import app
from pgqueuer.adapters.persistence.qb import DBSettings
from test.helpers import env_from_dsn


def test_cli_backlog_json_empty(dsn: str) -> None:
    env = os.environ.copy()
    env.update(env_from_dsn(dsn))

    result = CliRunner().invoke(app, ["backlog", "--json"], env=env)

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == []


def test_cli_backlog_json_counts_queued_per_entrypoint(
    dsn: str,
    pgdriver: db.SyncDriver,
) -> None:
    env = os.environ.copy()
    env.update(env_from_dsn(dsn))
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
