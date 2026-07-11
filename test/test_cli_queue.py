from __future__ import annotations

import os

from typer.testing import CliRunner

from pgqueuer import db, queries
from pgqueuer.adapters.cli.cli import app
from test.helpers import env_from_dsn


def test_cli_queue_dedupe_key_and_on_conflict(dsn: str, pgdriver: db.SyncDriver) -> None:
    runner = CliRunner()
    env = os.environ.copy()
    env.update(env_from_dsn(dsn))

    result = runner.invoke(app, ["queue", "ep", "payload", "--dedupe-key", "k"], env=env)
    assert result.exit_code == 0, result.output
    assert "Enqueued job" in result.output

    result = runner.invoke(
        app,
        ["queue", "ep", "payload", "--dedupe-key", "k", "--on-conflict", "skip"],
        env=env,
    )
    assert result.exit_code == 0, result.output
    assert "Skipped" in result.output

    result = runner.invoke(app, ["queue", "ep", "payload", "--dedupe-key", "k"], env=env)
    assert result.exit_code == 1, result.output
    assert "duplicate dedupe_key" in result.output

    assert sum(x.count for x in queries.SyncQueries(pgdriver).queue_size()) == 1
