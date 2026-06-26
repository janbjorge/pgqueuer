"""pgq upgrade widens legacy int4 id columns to BIGINT (#671)."""

from __future__ import annotations

from pgqueuer import db, queries
from pgqueuer.adapters.persistence import qb

SETTINGS = qb.DBSettings()
WIDENED_TABLES = [
    SETTINGS.queue_table,
    SETTINGS.statistics_table,
    SETTINGS.schedules_table,
]


async def _id_data_type(driver: db.Driver, table: str) -> str:
    rows = await driver.fetch(
        """SELECT data_type FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = $1
          AND column_name = 'id';""",
        table,
    )
    return rows[0]["data_type"]


async def test_upgrade_widens_legacy_int_id_columns(apgdriver: db.Driver) -> None:
    """A pre-existing int4 id is migrated to bigint, and re-running is a no-op."""
    q = queries.Queries(apgdriver)

    # Simulate a legacy install whose id columns are still int4 SERIAL.
    for table in WIDENED_TABLES:
        await apgdriver.execute(f"ALTER TABLE {table} ALTER COLUMN id TYPE INTEGER;")
        assert await _id_data_type(apgdriver, table) == "integer"

    await q.upgrade()
    for table in WIDENED_TABLES:
        assert await _id_data_type(apgdriver, table) == "bigint"

    # Idempotent: a second upgrade leaves the already-widened columns alone.
    await q.upgrade()
    for table in WIDENED_TABLES:
        assert await _id_data_type(apgdriver, table) == "bigint"
