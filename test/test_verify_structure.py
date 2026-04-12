from __future__ import annotations

import pytest

from pgqueuer import db
from pgqueuer.qm import QueueManager


async def test_verify_structure_passes_with_schema(apgdriver: db.Driver) -> None:
    """verify_structure must pass when the schema is installed."""
    qm = QueueManager(apgdriver)
    # Should not raise — schema is installed by the dsn fixture.
    await qm.verify_structure()


async def test_verify_structure_fails_missing_column(apgdriver: db.Driver) -> None:
    """verify_structure must raise when a required column is missing."""
    qm = QueueManager(apgdriver)
    table = qm.queries.qbe.settings.queue_table

    await apgdriver.execute(f"ALTER TABLE {table} DROP COLUMN heartbeat")

    with pytest.raises(RuntimeError, match="heartbeat"):
        await qm.verify_structure()


async def test_verify_structure_fails_missing_table(apgdriver: db.Driver) -> None:
    """verify_structure must raise when a required table is missing."""
    qm = QueueManager(apgdriver)
    table = qm.queries.qbe.settings.queue_table_log

    await apgdriver.execute(f"DROP TABLE {table}")

    with pytest.raises(RuntimeError, match=table):
        await qm.verify_structure()
