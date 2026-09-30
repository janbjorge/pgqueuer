from __future__ import annotations

import json
import os

from typer.testing import CliRunner

from pgqueuer import db, queries
from pgqueuer.adapters.cli.cli import app
from pgqueuer.adapters.persistence.qb import DBSettings
from test.helpers import env_from_dsn


def test_cli_failed_json_empty(dsn: str) -> None:
    env = os.environ.copy()
    env.update(env_from_dsn(dsn))

    result = CliRunner().invoke(app, ["failed", "--json"], env=env)

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == []


def test_cli_failed_json_reports_payload_size(dsn: str, pgdriver: db.SyncDriver) -> None:
    env = os.environ.copy()
    env.update(env_from_dsn(dsn))
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
