"""pgq upgrade modernizes legacy int4 SERIAL id columns to BIGINT IDENTITY (#671)."""

from __future__ import annotations

from pgqueuer import db, queries
from test.helpers import (
    WIDENED_ID_TABLES,
    id_data_type,
    id_has_serial_sequence,
    id_is_identity,
    simulate_legacy_serial,
)

INT4_MAX = 2**31 - 1


async def test_upgrade_converts_legacy_int_id_to_bigint_identity(apgdriver: db.Driver) -> None:
    """A legacy int4 SERIAL id becomes a bigint IDENTITY; re-running is a no-op."""
    q = queries.Queries(apgdriver)

    for table in WIDENED_ID_TABLES:
        await simulate_legacy_serial(apgdriver, table)
        assert await id_data_type(apgdriver, table) == "integer"
        assert not await id_is_identity(apgdriver, table)

    await q.upgrade()
    for table in WIDENED_ID_TABLES:
        assert await id_data_type(apgdriver, table) == "bigint"
        assert await id_is_identity(apgdriver, table)
        # The legacy SERIAL sequence is replaced by the identity's own; no leftover.
        assert not await id_has_serial_sequence(apgdriver, table)

    # Idempotent: a second upgrade leaves the already-converted columns alone.
    await q.upgrade()
    for table in WIDENED_ID_TABLES:
        assert await id_data_type(apgdriver, table) == "bigint"
        assert await id_is_identity(apgdriver, table)


async def test_fresh_install_id_is_bigint_identity(apgdriver: db.Driver) -> None:
    """A fresh install already uses bigint IDENTITY, matching the upgraded shape."""
    for table in WIDENED_ID_TABLES:
        assert await id_data_type(apgdriver, table) == "bigint"
        assert await id_is_identity(apgdriver, table)
        assert not await id_has_serial_sequence(apgdriver, table)


async def test_upgrade_preserves_id_number_line(apgdriver: db.Driver) -> None:
    """Conversion never rewinds the id sequence below the legacy high-water mark."""
    q = queries.Queries(apgdriver)
    table = WIDENED_ID_TABLES[0]
    await simulate_legacy_serial(apgdriver, table)
    await apgdriver.execute(f"SELECT setval('{table}_id_seq', {INT4_MAX - 1});")

    await q.upgrade()

    # The new identity sequence continues past int4 max instead of restarting at 1.
    ids = await q.enqueue(["ep"] * 2, [b"a", b"b"], [0] * 2)
    assert [int(x) for x in ids] == [INT4_MAX, INT4_MAX + 1]
