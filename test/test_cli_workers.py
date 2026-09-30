from __future__ import annotations

import json
import os
import uuid

from typer.testing import CliRunner

from pgqueuer import db, queries
from pgqueuer.adapters.cli.cli import app
from pgqueuer.adapters.persistence.qb import DBSettings
from test.helpers import env_from_dsn


def test_cli_workers_json_empty(dsn: str) -> None:
    env = os.environ.copy()
    env.update(env_from_dsn(dsn))

    result = CliRunner().invoke(app, ["workers", "--json"], env=env)

    assert result.exit_code == 0, result.output
    assert json.loads(result.stdout) == []


def test_cli_workers_json_groups_picked_jobs(dsn: str, pgdriver: db.SyncDriver) -> None:
    env = os.environ.copy()
    env.update(env_from_dsn(dsn))
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
