"""Effect-based checks for first-class schema support (``DBSettings.db_schema``).

These run against a real database and assert on resulting objects/behavior,
never on generated SQL text.
"""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import asyncpg
import pytest
from async_timeout import timeout

from pgqueuer import db, queries
from pgqueuer.core.listeners import initialize_notice_event_listener
from pgqueuer.domain.settings import DBSettings
from pgqueuer.models import AnyEvent, Channel, CronExpressionEntrypoint
from pgqueuer.queries import EntrypointExecutionParameter
from pgqueuer.types import CronEntrypoint, CronExpression
from test.helpers import id_data_type, queries_for, simulate_legacy_serial

SCHEMA = "pgq_iso"


async def table_schemas(driver: db.Driver, table: str) -> set[str]:
    rows = await driver.fetch(
        """SELECT n.nspname FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relname = $1 AND c.relkind = 'r'""",
        table,
    )
    return {row["nspname"] for row in rows}


async def enqueue_dequeue_round_trip(q: queries.Queries) -> None:
    (job_id,) = await q.enqueue("ep", b"x", 0)
    jobs = await q.dequeue(
        batch_size=10,
        entrypoints={"ep": EntrypointExecutionParameter(0)},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=None,
        heartbeat_timeout=timedelta(seconds=30),
    )
    assert [j.id for j in jobs] == [job_id]


async def test_search_path_install_upgrades_via_db_schema(apgdriver: db.Driver) -> None:
    """A legacy search_path install is addressable by setting db_schema to the same schema."""
    await queries.Queries(apgdriver).uninstall()
    await apgdriver.execute(f"CREATE SCHEMA {SCHEMA};")
    await apgdriver.execute(f"SET search_path TO {SCHEMA};")
    await queries.Queries(apgdriver).install()
    await apgdriver.execute("SET search_path TO public;")

    q = queries_for(apgdriver, DBSettings(db_schema=SCHEMA))
    assert await q.has_table(q.qbe.settings.queue_table)
    await q.upgrade()
    await q.upgrade()
    await enqueue_dequeue_round_trip(q)


async def test_fresh_install_round_trip_in_schema(apgdriver: db.Driver) -> None:
    """install() creates the schema and a fully working queue isolated from public."""
    await queries.Queries(apgdriver).uninstall()

    settings = DBSettings(db_schema=SCHEMA, prefix="iso_")
    q = queries_for(apgdriver, settings)
    await q.install()

    assert await table_schemas(apgdriver, settings.queue_table) == {SCHEMA}
    await enqueue_dequeue_round_trip(q)
    key = CronExpressionEntrypoint(CronEntrypoint("sched_ep"), CronExpression("* * * * *"))
    await q.insert_schedule({key: timedelta(seconds=60)})
    assert len(await q.peek_schedule()) == 1

    await q.uninstall()
    assert await table_schemas(apgdriver, settings.queue_table) == set()
    # The schema itself is left in place; it may hold unrelated objects.
    rows = await apgdriver.fetch(
        "SELECT 1 FROM pg_namespace WHERE nspname = $1",
        SCHEMA,
    )
    assert len(rows) == 1


async def test_widen_id_in_schema_with_prefix(apgdriver: db.Driver) -> None:
    """The widen-id migration targets the configured schema, not the search_path."""
    await queries.Queries(apgdriver).uninstall()

    settings = DBSettings(db_schema=SCHEMA, prefix="iso_")
    q = queries_for(apgdriver, settings)
    await q.install()

    widened = [settings.queue_table, settings.statistics_table, settings.schedules_table]
    for table in widened:
        await simulate_legacy_serial(apgdriver, table, schema=SCHEMA)

    await q.upgrade()

    for table in widened:
        assert await id_data_type(apgdriver, table, schema=SCHEMA) == "bigint"
    await enqueue_dequeue_round_trip(q)


async def test_notify_channel_is_not_schema_qualified(apgdriver: db.Driver) -> None:
    """With db_schema set, events still arrive on the bare channel with the bare table name."""
    await queries.Queries(apgdriver).uninstall()

    settings = DBSettings(db_schema=SCHEMA)
    q = queries_for(apgdriver, settings)
    await q.install()

    events = list[AnyEvent]()
    await initialize_notice_event_listener(apgdriver, Channel(settings.channel), events.append)
    await q.enqueue("ep", None)

    async with timeout(1):
        while len(events) < 1:
            await asyncio.sleep(0)

    (event,) = events
    assert event.root.type == "table_changed_event"
    assert event.root.table == settings.queue_table


async def test_schema_info_respects_db_schema(apgdriver: db.Driver) -> None:
    await queries.Queries(apgdriver).uninstall()

    settings = DBSettings(db_schema=SCHEMA)
    q = queries_for(apgdriver, settings)
    await q.install()

    rows = await apgdriver.fetch(q.qbq.build_schema_info_query())
    assert {row["table_name"] for row in rows} == {
        settings.queue_table,
        settings.queue_table_log,
        settings.statistics_table,
        settings.schedules_table,
    }


async def test_install_without_create_schema(apgdriver: db.Driver) -> None:
    """install(create_schema=False) requires a pre-existing schema."""
    await queries.Queries(apgdriver).uninstall()

    settings = DBSettings(db_schema=SCHEMA)
    q = queries_for(apgdriver, settings)

    with pytest.raises(asyncpg.InvalidSchemaNameError):
        await q.install(create_schema=False)

    await apgdriver.execute(f"CREATE SCHEMA {SCHEMA};")
    await q.install(create_schema=False)
    assert await q.has_table(settings.queue_table)
