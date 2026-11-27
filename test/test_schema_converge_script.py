"""The offline converge script, applied to a real database rather than read as text."""

from __future__ import annotations

import asyncio

import asyncpg
import pytest

from pgqueuer.adapters.persistence import schema_ddl
from pgqueuer.adapters.persistence.query_helpers import cell
from pgqueuer.adapters.persistence.schema_inspect import inspect
from pgqueuer.adapters.persistence.schema_plan import plan
from pgqueuer.db import AsyncpgDriver, AsyncpgPoolDriver
from pgqueuer.domain.schema.declaration import target
from pgqueuer.domain.settings import DBSettings
from pgqueuer.queries import Queries
from test.helpers import RELEASES_DIR, install_release

RELEASES = sorted(path.stem for path in RELEASES_DIR.glob("*.sql"))

# The lock modes that conflict with ROW EXCLUSIVE, which every enqueue and dequeue takes.
BLOCKS_WRITERS = ["ShareLock", "ShareRowExclusiveLock", "ExclusiveLock", "AccessExclusiveLock"]

QUEUED_FOR_A_LOCK = "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted)"


async def apply_converge_script(driver: AsyncpgDriver, settings: DBSettings) -> None:
    """One statement per round trip, as ``pgq sql upgrade | psql`` would."""
    for statement in schema_ddl.render_converge(settings):
        await driver.execute(statement)


async def locks_that_block_writers(driver: AsyncpgDriver, settings: DBSettings) -> list[str]:
    """Apply the script one transaction per statement, as psql does, and list what blocks writers.

    A statement holds its locks until COMMIT, so pg_locks still shows them before it.
    """
    held: list[str] = []
    for position, statement in enumerate(schema_ddl.render_converge(settings)):
        await driver.execute("BEGIN")
        await driver.execute(statement)
        rows = await driver.fetch(
            """SELECT relation::regclass::text AS relation, mode FROM pg_locks
            WHERE pid = pg_backend_pid() AND locktype = 'relation' AND mode = ANY($1::text[])""",
            BLOCKS_WRITERS,
        )
        await driver.execute("COMMIT")
        summary = " ".join(statement.split())[:100]
        held += [
            f"#{position} {cell(row, 'mode', str)} on {cell(row, 'relation', str)}: {summary}"
            for row in rows
        ]
    return held


async def plan_left(driver: AsyncpgDriver, settings: DBSettings) -> tuple[str, ...]:
    """Statements the connected upgrade would still apply."""
    return plan(await inspect(driver, settings), target(settings), settings).statements


async def relfilenode(driver: AsyncpgDriver, relation: str) -> int:
    """Changes whenever a table or index is physically rebuilt."""
    rows = await driver.fetch(
        "SELECT relfilenode FROM pg_class WHERE relname = $1",
        relation,
    )
    return cell(rows[0], "relfilenode", int)


async def index_definition(driver: AsyncpgDriver, index: str) -> str:
    rows = await driver.fetch(
        """SELECT pg_get_indexdef(index_class.oid) AS definition
        FROM pg_class index_class
        WHERE index_class.relname = $1 AND index_class.relkind = 'i'""",
        index,
    )
    return cell(rows[0], "definition", str)


async def test_the_script_does_not_rewrite_a_current_statistics_table(
    apgdriver: AsyncpgDriver,
) -> None:
    """The retype is guarded, so re-applying the script is not a table rewrite.

    Unguarded it fired every run, rewriting the table under ACCESS EXCLUSIVE
    for anyone piping ``pgq sql upgrade`` at an already-current database.
    """
    settings = DBSettings()
    before = await relfilenode(apgdriver, settings.statistics_table)

    await apply_converge_script(apgdriver, settings)

    assert await relfilenode(apgdriver, settings.statistics_table) == before


async def test_the_script_does_not_rebuild_current_redefined_indexes(
    apgdriver: AsyncpgDriver,
) -> None:
    settings = DBSettings()
    names = (
        f"{settings.queue_table_log}_not_aggregated",
        f"{settings.statistics_table}_unique_count",
    )
    before = {name: await relfilenode(apgdriver, name) for name in names}

    await apply_converge_script(apgdriver, settings)

    assert {name: await relfilenode(apgdriver, name) for name in names} == before


async def test_the_script_rebuilds_a_stale_redefined_index(
    apgdriver: AsyncpgDriver,
) -> None:
    settings = DBSettings()
    name = f"{settings.queue_table_log}_not_aggregated"
    await apgdriver.execute(f"DROP INDEX {name};")
    await apgdriver.execute(
        f"CREATE INDEX {name} ON {settings.queue_table_log} USING btree (created);"
    )

    await apply_converge_script(apgdriver, settings)

    definition = await index_definition(apgdriver, name)
    assert "USING btree ((1)) WHERE (NOT aggregated)" in definition


async def test_the_script_rebuilds_a_stale_unique_count_index_once(
    apgdriver: AsyncpgDriver,
) -> None:
    """The ON CONFLICT arbiter is restored."""
    settings = DBSettings()
    name = f"{settings.statistics_table}_unique_count"
    await apgdriver.execute(f"DROP INDEX {name};")
    await apgdriver.execute(
        f"CREATE UNIQUE INDEX {name} ON {settings.statistics_table} "
        "(priority, DATE_TRUNC('sec', created at time zone 'UTC'), status, entrypoint);"
    )

    await apply_converge_script(apgdriver, settings)

    assert "timezone('UTC'::text, created)" in await index_definition(apgdriver, name)


async def test_the_script_still_converges_a_legacy_status_type(
    apgdriver: AsyncpgDriver,
) -> None:
    """The guard must not cost the conversion it guards."""
    settings = DBSettings()
    legacy = settings.legacy_statistics_status_type
    await apgdriver.execute(f"DROP TYPE IF EXISTS {legacy} CASCADE;")
    await apgdriver.execute(
        f"CREATE TYPE {legacy} AS ENUM ('exception', 'successful', 'canceled');"
    )
    await apgdriver.execute(
        f"ALTER TABLE {settings.statistics_table} ALTER COLUMN status "
        f"TYPE {legacy} USING status::TEXT::{legacy};"
    )

    await apply_converge_script(apgdriver, settings)

    rows = await apgdriver.fetch(
        """SELECT udt_name FROM information_schema.columns
        WHERE table_name = $1 AND column_name = 'status'""",
        settings.statistics_table,
    )
    assert cell(rows[0], "udt_name", str) == settings.queue_status_type


async def test_the_script_folds_split_statistics_buckets(apgdriver: AsyncpgDriver) -> None:
    """Offline, like online, the unique index cannot build over split buckets."""
    settings = DBSettings()
    await install_release(apgdriver, "v0.18.10")
    await apgdriver.execute(
        """
        INSERT INTO pgqueuer_statistics
            (created, count, priority, time_in_queue, status, entrypoint)
        VALUES
            ('2024-01-01 00:00:00.100+00', 1, 0, '1 second', 'successful', 'split'),
            ('2024-01-01 00:00:00.200+00', 2, 0, '2 seconds', 'successful', 'split'),
            ('2024-01-01 00:00:00.000+00', 5, 0, '1 second', 'successful', 'alone')
        """
    )

    await apply_converge_script(apgdriver, settings)
    await apply_converge_script(apgdriver, settings)

    rows = await apgdriver.fetch(
        "SELECT entrypoint, count FROM pgqueuer_statistics ORDER BY entrypoint"
    )
    assert [(cell(row, "entrypoint", str), cell(row, "count", int)) for row in rows] == [
        ("alone", 5),
        ("split", 3),
    ]


async def test_a_rerun_takes_no_lock_that_blocks_writers(apgdriver: AsyncpgDriver) -> None:
    """On a current database, no statement takes a lock that conflicts with ROW EXCLUSIVE."""
    assert await locks_that_block_writers(apgdriver, DBSettings()) == []


@pytest.mark.parametrize("release", RELEASES)
async def test_a_rerun_after_an_upgrade_takes_no_lock_that_blocks_writers(
    apgdriver: AsyncpgDriver,
    release: str,
) -> None:
    """The guards agree with what the first run leaves, retired columns included."""
    settings = DBSettings()
    await install_release(apgdriver, release)
    await apply_converge_script(apgdriver, settings)

    assert await locks_that_block_writers(apgdriver, settings) == []


@pytest.mark.parametrize("search_path", ["public", "pgq_rerun, public"])
async def test_a_rerun_in_a_schema_takes_no_lock_that_blocks_writers(
    apgdriver: AsyncpgDriver,
    search_path: str,
) -> None:
    """A default reads back qualified off search_path and bare on it; both count as current."""
    settings = DBSettings(db_schema="pgq_rerun")
    await Queries(apgdriver, settings).install()
    await apgdriver.execute(f"SET search_path TO {search_path}")

    assert await locks_that_block_writers(apgdriver, settings) == []


async def test_a_rerun_lets_an_enqueue_pass_an_open_reader(
    apgdriver: AsyncpgDriver,
    dsn: str,
) -> None:
    """An open transaction that has read the queue table holds up neither a rerun nor an enqueue."""
    settings = DBSettings()
    rows = await apgdriver.fetch("SELECT pg_backend_pid() AS pid")
    runner = cell(rows[0], "pid", int)
    async with (
        asyncpg.create_pool(dsn=dsn, min_size=2, max_size=2) as pool,
        pool.acquire() as reader,
        asyncio.TaskGroup() as tasks,
        reader.transaction(),
    ):
        await reader.execute(f"SELECT count(*) FROM {settings.queue_table}")
        script = tasks.create_task(apply_converge_script(apgdriver, settings))
        # Enqueue once the script has ended or queues for a lock, not after a fixed delay.
        async with asyncio.timeout(5):
            while not script.done() and not await reader.fetchval(QUEUED_FOR_A_LOCK, runner):
                await asyncio.wait({script}, timeout=0.01)
        enqueue = tasks.create_task(Queries(AsyncpgPoolDriver(pool)).enqueue("rerun", None))
        await asyncio.wait({enqueue}, timeout=1)
        enqueued_beside_reader = enqueue.done()
        await asyncio.wait({script}, timeout=5)
        script_ended_beside_reader = script.done()

    assert enqueued_beside_reader, "the enqueue queued behind the script"
    assert script_ended_beside_reader, "the script queued behind the reader"


@pytest.mark.parametrize("release", RELEASES)
async def test_the_script_leaves_nothing_to_plan_on_any_release(
    apgdriver: AsyncpgDriver,
    release: str,
) -> None:
    """The guards skip only work that is done: one run reaches the declaration."""
    settings = DBSettings()
    await install_release(apgdriver, release)

    await apply_converge_script(apgdriver, settings)

    assert await plan_left(apgdriver, settings) == ()


async def test_the_script_restores_a_dropped_trigger(apgdriver: AsyncpgDriver) -> None:
    """The trigger guard still creates the trigger when it is missing."""
    settings = DBSettings()
    await apgdriver.execute(f"DROP TRIGGER {settings.trigger} ON {settings.queue_table};")

    await apply_converge_script(apgdriver, settings)

    assert await plan_left(apgdriver, settings) == ()
