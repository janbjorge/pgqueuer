from __future__ import annotations

import asyncio
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone
from itertools import count
from typing import Protocol
from urllib.parse import urlparse

import async_timeout

from pgqueuer import db
from pgqueuer.adapters.persistence import qb
from pgqueuer.models import Job
from pgqueuer.ports import RepositoryPort

# Tables whose id was int4 SERIAL before #671 and gets widened to BIGINT.
WIDENED_ID_TABLES = [
    qb.DBSettings().queue_table,
    qb.DBSettings().statistics_table,
    qb.DBSettings().schedules_table,
]


async def id_data_type(driver: db.Driver, table: str) -> str:
    """SQL data_type of ``table.id`` in the current schema."""
    rows = await driver.fetch(
        """SELECT data_type FROM information_schema.columns
        WHERE table_schema = current_schema()
          AND table_name = $1
          AND column_name = 'id';""",
        table,
    )
    return rows[0]["data_type"]


async def simulate_legacy_serial(driver: db.Driver, table: str) -> None:
    """Recreate the pre-#671 state: int4 column backed by an int4 SERIAL sequence.

    Resets from any current shape (BIGSERIAL or identity) so it is stable
    regardless of what the install builder produces.
    """
    seq = f"{table}_id_seq"
    await driver.execute(f"ALTER TABLE {table} ALTER COLUMN id DROP IDENTITY IF EXISTS;")
    await driver.execute(f"ALTER TABLE {table} ALTER COLUMN id DROP DEFAULT;")
    await driver.execute(f"DROP SEQUENCE IF EXISTS {seq};")
    await driver.execute(f"ALTER TABLE {table} ALTER COLUMN id TYPE INTEGER;")
    await driver.execute(f"CREATE SEQUENCE {seq} AS INTEGER OWNED BY {table}.id;")
    await driver.execute(f"ALTER TABLE {table} ALTER COLUMN id SET DEFAULT nextval('{seq}');")


def env_from_dsn(dsn: str) -> dict[str, str]:
    """Build the PG* environment variables asyncpg/psycopg expect from a DSN."""
    parsed = urlparse(dsn)
    return {
        "PGHOST": parsed.hostname or "localhost",
        "PGPORT": str(parsed.port or 5432),
        "PGUSER": parsed.username or "",
        "PGPASSWORD": parsed.password or "",
        "PGDATABASE": parsed.path.lstrip("/") or "",
    }


class ShutdownCapable(Protocol):
    shutdown: asyncio.Event


def mocked_job(
    id: int | count = count(),
    priority: int = 1,
    created: datetime | None = None,
    heartbeat: datetime | None = None,
    execute_after: datetime | None = None,
    updated: datetime | None = None,
    status: str = "queued",
    entrypoint: str = "test",
    payload: bytes | None = None,
    queue_manager_id: None | uuid.UUID = None,
    headers: dict | None = None,
) -> Job:
    now = datetime.now(timezone.utc)
    return Job(
        id=id if isinstance(id, int) else next(id),
        priority=priority,
        created=created or now,
        heartbeat=heartbeat or now,
        execute_after=execute_after or now,
        updated=updated or now,
        status=status,
        entrypoint=entrypoint,
        payload=payload,
        queue_manager_id=queue_manager_id or uuid.uuid4(),
        headers=headers,
    )


async def wait_until_empty_queue(
    queries: RepositoryPort,
    managers: Sequence[ShutdownCapable],
    *,
    timeout_seconds: float = 15.0,
    poll_interval: float = 0.01,
) -> None:
    """
    Block until the queue is empty or the timeout elapses, then signal shutdown.
    """
    async with async_timeout.timeout(timeout_seconds):
        while sum(result.count for result in await queries.queue_size()) > 0:
            await asyncio.sleep(poll_interval)

    for manager in managers:
        manager.shutdown.set()
