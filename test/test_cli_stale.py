from __future__ import annotations

import json
import os

from typer.testing import CliRunner

from pgqueuer import db, queries
from pgqueuer.adapters.cli.cli import app
from pgqueuer.adapters.persistence.qb import DBSettings
from test.helpers import env_from_dsn


def test_cli_stale_json_empty(dsn: str) -> None:
    env = os.environ.copy()
    env.update(env_from_dsn(dsn))

    result = CliRunner().invoke(app, ["stale", "--json"], env=env)

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == []


def test_cli_stale_json_reports_old_heartbeat(dsn: str, pgdriver: db.SyncDriver) -> None:
    env = os.environ.copy()
    env.update(env_from_dsn(dsn))
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
