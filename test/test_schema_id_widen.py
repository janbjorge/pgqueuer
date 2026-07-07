"""pgq upgrade widens legacy int4 id columns to BIGINT (#671)."""

from __future__ import annotations

from pgqueuer import db, queries
from test.helpers import WIDENED_ID_TABLES, id_data_type, simulate_legacy_serial

INT4_MAX = 2**31 - 1


async def test_upgrade_widens_legacy_int_id_columns(apgdriver: db.Driver) -> None:
    """A pre-existing int4 id is migrated to bigint, and re-running is a no-op."""
    q = queries.Queries(apgdriver)

    # Simulate a legacy install whose id columns are still int4 SERIAL.
    for table in WIDENED_ID_TABLES:
        await simulate_legacy_serial(apgdriver, table)
        assert await id_data_type(apgdriver, table) == "integer"

    await q.upgrade()
    for table in WIDENED_ID_TABLES:
        assert await id_data_type(apgdriver, table) == "bigint"

    # Idempotent: a second upgrade leaves the already-widened columns alone.
    await q.upgrade()
    for table in WIDENED_ID_TABLES:
        assert await id_data_type(apgdriver, table) == "bigint"


async def test_upgrade_widens_legacy_serial_sequence(apgdriver: db.Driver) -> None:
    """Widening the column alone is not enough; the SERIAL sequence caps at 2^31-1 too."""
    q = queries.Queries(apgdriver)
    table = WIDENED_ID_TABLES[0]
    await simulate_legacy_serial(apgdriver, table)
    await apgdriver.execute(f"SELECT setval('{table}_id_seq', {INT4_MAX - 1});")

    await q.upgrade()

    ids = await q.enqueue(["ep"] * 2, [b"a", b"b"], [0] * 2)
    assert [int(x) for x in ids] == [INT4_MAX, INT4_MAX + 1]
