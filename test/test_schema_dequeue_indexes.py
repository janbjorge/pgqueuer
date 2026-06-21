"""Behavioral check that pgq upgrade delivers the dequeue indexes (#668)."""

from __future__ import annotations

from pgqueuer import db, queries
from pgqueuer.adapters.persistence import qb

QUEUE_TABLE = qb.DBSettings().queue_table
DEQUEUE_INDEXES = [f"{QUEUE_TABLE}_ep_prio_id_idx", f"{QUEUE_TABLE}_ep_ea_idx"]


async def test_upgrade_recreates_dropped_dequeue_indexes(apgdriver: db.Driver) -> None:
    """pgq upgrade re-creates the dequeue indexes when they are missing (#668)."""
    q = queries.Queries(apgdriver)
    for index in DEQUEUE_INDEXES:
        await apgdriver.execute(f"DROP INDEX {index};")
        assert not await q.table_has_index(QUEUE_TABLE, index)

    await q.upgrade()

    for index in DEQUEUE_INDEXES:
        assert await q.table_has_index(QUEUE_TABLE, index)
