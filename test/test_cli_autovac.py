from typer.testing import CliRunner

from pgqueuer.cli import app


def test_autovac_command() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["autovac", "--dry-run"])
    assert result.exit_code == 0


def test_autovac_rollback_command() -> None:
    runner = CliRunner()
    result = runner.invoke(app, ["autovac", "--dry-run", "--rollback"])
    assert result.exit_code == 0
