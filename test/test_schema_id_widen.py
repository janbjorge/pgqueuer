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
INT4_MAX = 2**31 - 1


async def _id_data_type(driver: db.Driver, table: str) -> str:
    rows = await driver.fetch(
        """SELECT data_type FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = $1
          AND column_name = 'id';""",
        table,
    )
    return rows[0]["data_type"]


async def _simulate_legacy_serial(driver: db.Driver, table: str) -> None:
    """Recreate the pre-#671 state: int4 column backed by an int4 SERIAL sequence."""
    seq = f"{table}_id_seq"
    await driver.execute(f"ALTER TABLE {table} ALTER COLUMN id DROP IDENTITY IF EXISTS;")
    await driver.execute(f"ALTER TABLE {table} ALTER COLUMN id TYPE INTEGER;")
    await driver.execute(f"CREATE SEQUENCE {seq} AS INTEGER OWNED BY {table}.id;")
    await driver.execute(f"ALTER TABLE {table} ALTER COLUMN id SET DEFAULT nextval('{seq}');")


async def test_upgrade_widens_legacy_int_id_columns(apgdriver: db.Driver) -> None:
    """A pre-existing int4 id is migrated to bigint, and re-running is a no-op."""
    q = queries.Queries(apgdriver)

    # Simulate a legacy install whose id columns are still int4 SERIAL.
    for table in WIDENED_TABLES:
        await _simulate_legacy_serial(apgdriver, table)
        assert await _id_data_type(apgdriver, table) == "integer"

    await q.upgrade()
    for table in WIDENED_TABLES:
        assert await _id_data_type(apgdriver, table) == "bigint"

    # Idempotent: a second upgrade leaves the already-widened columns alone.
    await q.upgrade()
    for table in WIDENED_TABLES:
        assert await _id_data_type(apgdriver, table) == "bigint"


async def test_upgrade_widens_legacy_serial_sequence(apgdriver: db.Driver) -> None:
    """Widening the column alone is not enough; the SERIAL sequence caps at 2^31-1 too."""
    q = queries.Queries(apgdriver)
    table = SETTINGS.queue_table
    await _simulate_legacy_serial(apgdriver, table)
    await apgdriver.execute(f"SELECT setval('{table}_id_seq', {INT4_MAX - 1});")

    await q.upgrade()

    ids = await q.enqueue(["ep"] * 2, [b"a", b"b"], [0] * 2)
    assert [int(x) for x in ids] == [INT4_MAX, INT4_MAX + 1]
