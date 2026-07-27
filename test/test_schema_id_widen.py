"""pgq upgrade widens legacy int4 id columns to BIGINT (#671)."""

from __future__ import annotations

from pgqueuer import db, queries
from test.helpers import id_data_type, simulate_legacy_serial, widened_id_tables

INT4_MAX = 2**31 - 1


async def test_upgrade_widens_legacy_int_id_columns(apgdriver: db.Driver) -> None:
    """A pre-existing int4 id is migrated to bigint, and re-running is a no-op."""
    q = queries.Queries(apgdriver)

    # Simulate a legacy install whose id columns are still int4 SERIAL.
    for table in widened_id_tables():
        await simulate_legacy_serial(apgdriver, table)
        assert await id_data_type(apgdriver, table) == "integer"

    await q.upgrade()
    for table in widened_id_tables():
        assert await id_data_type(apgdriver, table) == "bigint"

    # Idempotent: a second upgrade leaves the already-widened columns alone.
    await q.upgrade()
    for table in widened_id_tables():
        assert await id_data_type(apgdriver, table) == "bigint"


async def test_upgrade_widens_legacy_serial_sequence(apgdriver: db.Driver) -> None:
    """Widening the column alone is not enough; the SERIAL sequence caps at 2^31-1 too."""
    q = queries.Queries(apgdriver)
    table = widened_id_tables()[0]
    await simulate_legacy_serial(apgdriver, table)
    await apgdriver.execute(f"SELECT setval('{table}_id_seq', {INT4_MAX - 1});")

    await q.upgrade()

    ids = await q.enqueue(["ep"] * 2, [b"a", b"b"], [0] * 2)
    assert [int(x) for x in ids] == [INT4_MAX, INT4_MAX + 1]
