"""``Queries.upgrade`` applies the computed plan under an advisory lock."""

from __future__ import annotations

import logging

import pytest

from pgqueuer.adapters.persistence.query_helpers import cell
from pgqueuer.adapters.persistence.schema_plan import advisory_key
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.errors import SchemaDriftError
from pgqueuer.domain.settings import DBSettings
from pgqueuer.queries import Queries
from test.helpers import queries_for
from test.test_schema_convergence import install_release


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


async def test_a_converged_database_plans_nothing(apgdriver: AsyncpgDriver) -> None:
    """What lets `pgq upgrade` report itself converged rather than guessing."""
    queries = Queries(apgdriver)
    await queries.upgrade()

    assert (await queries.plan_upgrade()).statements == ()


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
