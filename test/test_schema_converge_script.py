"""The offline converge script, applied to a real database rather than read as text."""

from __future__ import annotations

from pgqueuer.adapters.persistence import schema_ddl
from pgqueuer.adapters.persistence.query_helpers import cell
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.settings import DBSettings
from pgqueuer.queries import Queries
from test.helpers import install_release


async def apply_converge_script(driver: AsyncpgDriver, settings: DBSettings) -> None:
    """One statement per round trip, as ``pgq sql upgrade | psql`` would."""
    for statement in schema_ddl.render_converge(settings):
        await driver.execute(statement)


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
    await Queries(apgdriver).upgrade()
    before = await relfilenode(apgdriver, settings.statistics_table)

    await apply_converge_script(apgdriver, settings)

    assert await relfilenode(apgdriver, settings.statistics_table) == before


async def test_the_script_does_not_rebuild_current_redefined_indexes(
    apgdriver: AsyncpgDriver,
) -> None:
    settings = DBSettings()
    await Queries(apgdriver).upgrade()
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
    """The ON CONFLICT arbiter is restored, and a re-apply leaves it alone.

    The second apply catches a compare that never matches its own CREATE,
    which would drop the index on every later run.
    """
    settings = DBSettings()
    name = f"{settings.statistics_table}_unique_count"
    await apgdriver.execute(f"DROP INDEX {name};")
    await apgdriver.execute(
        f"CREATE UNIQUE INDEX {name} ON {settings.statistics_table} "
        "(priority, DATE_TRUNC('sec', created at time zone 'UTC'), status, entrypoint);"
    )

    await apply_converge_script(apgdriver, settings)

    assert "timezone('UTC'::text, created)" in await index_definition(apgdriver, name)
    rebuilt = await relfilenode(apgdriver, name)

    await apply_converge_script(apgdriver, settings)

    assert await relfilenode(apgdriver, name) == rebuilt


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
