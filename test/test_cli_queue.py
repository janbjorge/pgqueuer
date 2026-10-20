from __future__ import annotations

import pytest
from typer.testing import CliRunner

from pgqueuer import db, queries
from pgqueuer.adapters.cli import cli
from pgqueuer.adapters.cli.cli import app
from test.helpers import env_from_dsn


def test_cli_queue_dedupe_key_and_on_conflict(dsn: str, pgdriver: db.SyncDriver) -> None:
    runner = CliRunner()
    env = env_from_dsn(dsn)

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
    assert "duplicate dedupe_key" in result.stderr
    assert result.stdout == ""

    assert sum(x.count for x in queries.SyncQueries(pgdriver).queue_size()) == 1


@pytest.mark.parametrize(
    ("args", "expected"),
    [
        (["listen"], "ch_pgqueuer"),
        (["--prefix", "foo_", "listen"], "foo_ch_pgqueuer"),
        (["--prefix", "foo_", "listen", "--channel", "custom"], "custom"),
    ],
)
def test_cli_listen_channel(
    dsn: str,
    monkeypatch: pytest.MonkeyPatch,
    args: list[str],
    expected: str,
) -> None:
    """The default channel follows --prefix, like the one PgQueuer notifies on."""
    monkeypatch.delenv("PGQUEUER_PREFIX", raising=False)
    heard: list[str] = []

    async def capture(driver: object, channel: str) -> None:
        heard.append(channel)

    monkeypatch.setattr(cli, "display_pg_channel", capture)
    result = CliRunner().invoke(app, args, env=env_from_dsn(dsn))
    assert result.exit_code == 0, result.output
    assert heard == [expected]
