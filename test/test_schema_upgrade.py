"""``Queries.upgrade`` applies the computed plan under an advisory lock."""

from __future__ import annotations

import logging
import re
from collections.abc import Sequence

import psycopg
import pytest
from typer.testing import CliRunner

from pgqueuer.adapters.cli.cli import app
from pgqueuer.adapters.persistence.query_helpers import cell
from pgqueuer.adapters.persistence.schema_inspect import inspect
from pgqueuer.adapters.persistence.schema_plan import advisory_key, plan, rebuild_name
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.errors import SchemaDriftError
from pgqueuer.domain.schema.declaration import target
from pgqueuer.domain.schema.model import Schema
from pgqueuer.domain.settings import DBSettings
from pgqueuer.queries import Queries
from test.helpers import RELEASES_DIR, env_from_dsn, install_release, queries_for

RELEASES = sorted(path.stem for path in RELEASES_DIR.glob("*.sql"))


async def advisory_locks_held(driver: AsyncpgDriver, settings: DBSettings) -> int:
    """How many sessions hold this installation's upgrade lock.

    A key that fits in 32 bits lands in ``objid`` with ``classid`` zero.
    """
    rows = await driver.fetch(
        """SELECT count(*) AS held FROM pg_locks
        WHERE locktype = 'advisory' AND classid = 0 AND objid = $1""",
        advisory_key(settings),
    )
    return cell(rows[0], "held", int)


async def test_upgrade_releases_its_lock(apgdriver: AsyncpgDriver) -> None:
    settings = DBSettings()
    assert await advisory_locks_held(apgdriver, settings) == 0

    await Queries(apgdriver).upgrade()

    assert await advisory_locks_held(apgdriver, settings) == 0


async def test_the_lock_is_released_when_planning_refuses(apgdriver: AsyncpgDriver) -> None:
    """The lock is held across plan-and-apply, so the failure path matters.

    Leaking it would wedge every later upgrade on this database.
    """
    settings = DBSettings()
    await apgdriver.execute(f"ALTER TABLE {settings.queue_table} ALTER COLUMN payload TYPE text;")

    with pytest.raises(SchemaDriftError):
        await Queries(apgdriver).upgrade()

    assert await advisory_locks_held(apgdriver, settings) == 0


async def test_a_scoped_install_plans_nothing(apgdriver: AsyncpgDriver) -> None:
    """The declaration is compared bare, as the catalog reports it.

    Comparing it qualified re-planned the whole schema on every upgrade, status
    column rewrite included, for anyone using ``db_schema``.
    """
    await apgdriver.execute("CREATE SCHEMA IF NOT EXISTS scoped_plan;")
    queries = queries_for(apgdriver, DBSettings(db_schema="scoped_plan", prefix="iso_"))
    await queries.install()

    assert (await queries.plan_upgrade()).statements == ()


async def test_upgrade_applies_exactly_what_it_planned(apgdriver: AsyncpgDriver) -> None:
    settings = DBSettings()
    name = f"{settings.queue_table}_ep_ea_idx"
    await apgdriver.execute(f"DROP INDEX {name};")

    queries = Queries(apgdriver)
    planned = await queries.plan_upgrade()
    assert len(planned.statements) == 1
    assert planned.statements[0].startswith(f"CREATE INDEX {name} ON")

    await queries.upgrade()
    assert (await queries.plan_upgrade()).statements == ()


def test_both_upgrade_commands_print_the_index_build_note(dsn: str) -> None:
    """Both commands print the note on stderr, and a converged database prints none."""
    settings = DBSettings()
    name = f"{settings.queue_table}_picked_heartbeat_idx"
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute(f"DROP INDEX {name}".encode())
    env = env_from_dsn(dsn)
    note = (
        f"note: Upgrading creates {name}. Postgres holds a SHARE lock on "
        f"{settings.queue_table} for the whole build, blocking writes to it"
    )

    planned = CliRunner().invoke(app, ["upgrade", "--plan"], env=env)
    applied = CliRunner().invoke(app, ["upgrade"], env=env)
    converged = CliRunner().invoke(app, ["upgrade"], env=env)

    assert (planned.exit_code, applied.exit_code, converged.exit_code) == (0, 0, 0)
    assert note in planned.stderr
    assert "note:" not in planned.stdout
    assert note in applied.stderr
    assert "note:" not in converged.stderr


def announced_builds(notes: Sequence[str]) -> set[str]:
    """``creates <index>`` or ``rebuilds <index>`` for each note that warns of a build."""
    return {
        found[1]
        for note in notes
        if (found := re.match(r"Upgrading ((?:creates|rebuilds) \S+)\. ", note))
    }


def builds_on_existing_tables(live: Schema, statements: Sequence[str]) -> set[str]:
    """The same form for each index build in *statements* on a table that *live* has."""
    rebuilt: dict[str, str] = {
        rebuild_name(index): index.name for index in target(DBSettings()).indexes
    }
    tables = {table.name for table in live.tables}
    builds = set[str]()
    for statement in statements:
        found = re.match(r"CREATE (?:UNIQUE )?INDEX (\S+) ON (\S+) ", statement)
        if found is not None and found[2] in tables:
            name = found[1]
            builds.add(f"rebuilds {rebuilt[name]}" if name in rebuilt else f"creates {name}")
    return builds


# v0.18.10 has no log table, so its upgrade also builds indexes on a new table.
@pytest.mark.parametrize("release", RELEASES)
async def test_each_index_build_on_an_existing_table_is_announced(
    apgdriver: AsyncpgDriver,
    release: str,
) -> None:
    """The notes name each build on a table that exists, and no build on a new table."""
    settings = DBSettings()
    await install_release(apgdriver, release)
    live = await inspect(apgdriver, settings)

    computed = plan(live, target(settings), settings)

    builds = builds_on_existing_tables(live, computed.statements)
    assert builds
    assert announced_builds(computed.notes) == builds


async def test_a_repointed_trigger_is_replaced_in_place(apgdriver: AsyncpgDriver) -> None:
    settings = DBSettings()
    await apgdriver.execute(
        "CREATE FUNCTION fn_other() RETURNS TRIGGER AS $$BEGIN RETURN NULL; END;$$ "
        "LANGUAGE plpgsql;"
    )
    await apgdriver.execute(
        f"DROP TRIGGER {settings.trigger} ON {settings.queue_table};"
        f"CREATE TRIGGER {settings.trigger} AFTER INSERT ON {settings.queue_table} "
        "EXECUTE FUNCTION fn_other();"
    )

    queries = Queries(apgdriver)
    planned = await queries.plan_upgrade()
    assert len(planned.statements) == 1
    assert planned.statements[0].startswith(f"CREATE OR REPLACE TRIGGER {settings.trigger}")

    await queries.upgrade()
    assert (await queries.plan_upgrade()).statements == ()


async def test_retired_column_notes_reach_the_log(
    apgdriver: AsyncpgDriver,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """A v0.18 database keeps time_in_queue; the operator should hear about it."""
    await install_release(apgdriver, "v0.18.10")

    with caplog.at_level(logging.WARNING, logger="pgqueuer"):
        await Queries(apgdriver).upgrade()

    assert any("time_in_queue" in record.getMessage() for record in caplog.records)


async def test_two_installations_do_not_block_each_other(apgdriver: AsyncpgDriver) -> None:
    """The key is derived from the qualified queue table, not from a constant."""
    default = DBSettings()
    other = DBSettings(prefix="acme_")
    assert advisory_key(default) != advisory_key(other)

    await queries_for(apgdriver, other).install()
    await queries_for(apgdriver, other).upgrade()
    assert await advisory_locks_held(apgdriver, other) == 0


@pytest.mark.parametrize(
    ("ours_present", "expected"),
    [
        pytest.param(True, (), id="ours-present"),
        pytest.param(
            False,
            (
                "CREATE TRIGGER tg_pgqueuer_changed\n"
                "AFTER INSERT OR UPDATE OR DELETE OR TRUNCATE ON pgqueuer\n"
                "EXECUTE FUNCTION fn_pgqueuer_changed();",
            ),
            id="ours-missing",
        ),
    ],
)
async def test_a_same_named_trigger_on_another_table_is_not_ours(
    apgdriver: AsyncpgDriver,
    ours_present: bool,
    expected: tuple[str, ...],
) -> None:
    """Trigger names are per table; another installation's must not stand in for ours."""
    other = DBSettings(
        queue_table="other_q",
        queue_table_log="other_q_log",
        statistics_table="other_q_stats",
        schedules_table="other_q_sched",
        queue_status_type="other_q_status",
        function="other_fn",
    )
    await queries_for(apgdriver, other).install()
    if not ours_present:
        await apgdriver.execute("DROP TRIGGER tg_pgqueuer_changed ON pgqueuer")

    assert (await Queries(apgdriver).plan_upgrade()).statements == expected


async def test_statistics_created_default_preserves_the_instant(
    apgdriver: AsyncpgDriver,
) -> None:
    """Omitting created stores this second's instant in a non-UTC session.

    ``timezone('UTC', now())`` is a timestamp without time zone. Storing it
    reads that wall clock in the session TimeZone and shifts the instant.
    """
    table = DBSettings().statistics_table
    await apgdriver.execute("BEGIN")
    await apgdriver.execute("SELECT set_config('TimeZone', 'America/New_York', true)")
    rows = await apgdriver.fetch(
        f"""
        INSERT INTO {table} (count, priority, status, entrypoint)
        VALUES (1, 0, 'successful', 'created-default')
        RETURNING created = date_trunc('sec', now()) AS correct
        """
    )
    await apgdriver.execute("COMMIT")
    assert cell(rows[0], "correct", bool) is True
