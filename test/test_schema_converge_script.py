"""The offline converge script, applied to a real database rather than read as text."""

from __future__ import annotations

from pgqueuer.adapters.persistence import qb
from pgqueuer.adapters.persistence.query_helpers import cell
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.settings import DBSettings
from pgqueuer.queries import Queries


async def apply_converge_script(driver: AsyncpgDriver, settings: DBSettings) -> None:
    """One statement per round trip, as ``pgq sql upgrade | psql`` would."""
    for statement in qb.QueryBuilderEnvironment(settings=settings).build_upgrade_queries():
        await driver.execute(statement)


async def relfilenode(driver: AsyncpgDriver, table: str) -> int:
    """Changes whenever the table is rewritten, which is what a retype does."""
    rows = await driver.fetch(
        "SELECT relfilenode FROM pg_class WHERE relname = $1 AND relkind = 'r'",
        table,
    )
    return cell(rows[0], "relfilenode", int)


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
