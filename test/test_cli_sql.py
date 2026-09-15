from __future__ import annotations

import pytest
from typer.testing import CliRunner

from pgqueuer.adapters.cli import cli
from pgqueuer.adapters.cli.cli import app
from pgqueuer.domain.schema.model import Plan

SQL_COMMANDS = [
    (["sql", "install"], "CREATE TYPE"),
    (["sql", "uninstall"], "DROP TABLE"),
    (["sql", "upgrade"], "ALTER TABLE"),
    (["sql", "durability", "volatile"], "SET UNLOGGED"),
    (["sql", "autovac"], "autovacuum_vacuum_scale_factor"),
]

DRY_RUN_ALIASES = [
    (["install", "--dry-run"], ["sql", "install"]),
    (["uninstall", "--dry-run"], ["sql", "uninstall"]),
    (["upgrade", "--dry-run"], ["sql", "upgrade"]),
    (["durability", "volatile", "--dry-run"], ["sql", "durability", "volatile"]),
    (["autovac", "--dry-run"], ["sql", "autovac"]),
    (["autovac", "--dry-run", "--rollback"], ["sql", "autovac", "--rollback"]),
]


@pytest.mark.parametrize(("args", "marker"), SQL_COMMANDS)
def test_sql_commands_emit_sql(args: list[str], marker: str) -> None:
    """SQL on stdout: expected statements, semicolon-terminated, one trailing newline,
    no dash separator lines."""
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output
    assert marker in result.output
    assert result.output.endswith(";\n")
    assert not result.output.endswith(";\n\n")
    assert not any(line.startswith("-----") for line in result.output.splitlines())


def test_sql_statements_are_left_aligned() -> None:
    """Top-level statements start at column 0; only inner structure (columns,
    plpgsql body) is indented."""
    uninstall = CliRunner().invoke(app, ["sql", "uninstall"]).output
    assert all(line.startswith("DROP") for line in uninstall.splitlines() if line.strip())

    install = CliRunner().invoke(app, ["sql", "install"]).output
    assert install.startswith("-- pgqueuer ")
    assert "\nCREATE TYPE" in install
    assert "\n    CREATE TABLE" not in install
    assert "\n    id bigserial" in install  # column indentation preserved


def test_sql_commands_are_deterministic() -> None:
    first = CliRunner().invoke(app, ["sql", "upgrade"]).output
    second = CliRunner().invoke(app, ["sql", "upgrade"]).output
    assert first == second


def test_sql_install_respects_prefix_and_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    # AppConfig.setup_env writes PGQUEUER_* into os.environ. Clear them for a
    # clean starting state; the autouse _restore_pgqueuer_env fixture in
    # conftest restores those keys afterward so the mutation cannot leak.
    monkeypatch.delenv("PGQUEUER_PREFIX", raising=False)
    monkeypatch.delenv("PGQUEUER_SCHEMA", raising=False)

    result = CliRunner().invoke(app, ["--prefix", "foo_", "--schema", "myschema", "sql", "install"])
    assert result.exit_code == 0, result.output
    assert "CREATE SCHEMA IF NOT EXISTS myschema;" in result.output
    assert "myschema.foo_pgqueuer" in result.output

    no_create = CliRunner().invoke(
        app, ["--prefix", "foo_", "--schema", "myschema", "sql", "install", "--no-create-schema"]
    )
    assert "CREATE SCHEMA" not in no_create.output


def test_sql_install_durability_option() -> None:
    result = CliRunner().invoke(app, ["sql", "install", "-d", "volatile"])
    assert "CREATE UNLOGGED TABLE" in result.output


def test_sql_upgrade_widen_id_option() -> None:
    """Assert on the widening itself; DO blocks guard other converge steps too."""
    with_widen = CliRunner().invoke(app, ["sql", "upgrade"]).output
    without_widen = CliRunner().invoke(app, ["sql", "upgrade", "--no-widen-id"]).output
    assert "ALTER COLUMN id TYPE BIGINT" in with_widen
    assert "ALTER SEQUENCE %s AS BIGINT" in with_widen
    assert "ALTER COLUMN id TYPE BIGINT" not in without_widen
    assert "ALTER SEQUENCE %s AS BIGINT" not in without_widen


def test_upgrade_takes_no_durability_option() -> None:
    """It never applied one: switching LOGGED/UNLOGGED rewrites the table.

    ``pgq sql upgrade`` keeps the flag, since the offline script renders the
    tables it may have to create.
    """
    rejected = CliRunner().invoke(app, ["upgrade", "--durability", "volatile"])
    assert rejected.exit_code != 0
    assert "No such option" in rejected.output

    assert CliRunner().invoke(app, ["sql", "upgrade", "-d", "volatile"]).exit_code == 0


def test_sql_autovac_rollback_option() -> None:
    result = CliRunner().invoke(app, ["sql", "autovac", "--rollback"])
    assert "RESET" in result.output


@pytest.mark.parametrize(
    "args",
    [alias for alias, _ in DRY_RUN_ALIASES] + [sql for _, sql in DRY_RUN_ALIASES],
)
def test_sql_commands_never_connect(monkeypatch: pytest.MonkeyPatch, args: list[str]) -> None:
    def raiser(*args: object) -> None:
        raise AssertionError("sql emission must not reach the event loop")

    monkeypatch.setattr(cli, "asyncio_run", raiser)
    result = CliRunner().invoke(app, args)
    assert result.exit_code == 0, result.output


@pytest.mark.parametrize(("alias_args", "sql_args"), DRY_RUN_ALIASES)
def test_dry_run_alias_matches_sql_command(alias_args: list[str], sql_args: list[str]) -> None:
    alias = CliRunner().invoke(app, alias_args)
    sql = CliRunner().invoke(app, sql_args)
    assert alias.exit_code == 0
    assert alias.stdout == sql.stdout
    assert "--dry-run is deprecated" in alias.stderr
    assert f"pgq sql {alias_args[0]}" in alias.stderr


@pytest.mark.parametrize("command", ["install", "uninstall", "upgrade", "durability", "autovac"])
def test_dry_run_hidden_from_help(command: str) -> None:
    result = CliRunner().invoke(app, [command, "--help"])
    assert result.exit_code == 0
    assert "--dry-run" not in result.output


@pytest.mark.parametrize(
    ("statements", "planned_only", "expected"),
    [
        ((), False, "PgQueuer schema is already up to date."),
        (("CREATE INDEX a ON t (c);",), False, "Applied 1 statement."),
        (("a;", "b;"), False, "Applied 2 statements."),
        (("a;", "b;"), True, "Would apply 2 statements."),
    ],
)
def test_report_upgrade_summarises_on_stderr(
    capsys: pytest.CaptureFixture[str],
    statements: tuple[str, ...],
    planned_only: bool,
    expected: str,
) -> None:
    """Summary and notes go to stderr so stdout carries only SQL."""
    cli.report_upgrade(Plan(statements=statements), planned_only=planned_only)

    captured = capsys.readouterr()
    assert captured.out == ""
    assert expected in captured.err


def test_report_upgrade_prefixes_notes(capsys: pytest.CaptureFixture[str]) -> None:
    cli.report_upgrade(Plan(notes=("time_in_queue is unused",)), planned_only=False)

    captured = capsys.readouterr()
    assert "note: time_in_queue is unused" in captured.err
    assert "already up to date" in captured.err
