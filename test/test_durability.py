from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
import pytest

from pgqueuer.adapters.persistence.query_helpers import cell
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.settings import DBSettings, Durability, DurabilityPolicy
from pgqueuer.queries import Queries


@pytest.mark.parametrize(
    "durability,expected_policy",
    [
        (
            Durability.volatile,
            DurabilityPolicy(
                queue_table="UNLOGGED",
                queue_log_table="UNLOGGED",
                statistics_table="UNLOGGED",
                schedules_table="UNLOGGED",
            ),
        ),
        (
            Durability.balanced,
            DurabilityPolicy(
                queue_table="",
                queue_log_table="UNLOGGED",
                statistics_table="UNLOGGED",
                schedules_table="",
            ),
        ),
        (
            Durability.durable,
            DurabilityPolicy(
                queue_table="",
                queue_log_table="",
                statistics_table="",
                schedules_table="",
            ),
        ),
    ],
)
def test_durability_policy(durability: Durability, expected_policy: DurabilityPolicy) -> None:
    """
    Test that the `config` property of each `Durability` level correctly returns
    the expected `DurabilityPolicy`.
    """
    assert durability.config == expected_policy


@pytest.mark.parametrize(
    "policy,expected_logged",
    [
        (
            DurabilityPolicy(
                queue_table="",
                queue_log_table="UNLOGGED",
                statistics_table="UNLOGGED",
                schedules_table="",
            ),
            {
                "pgqueuer": True,
                "pgqueuer_log": False,
                "pgqueuer_statistics": False,
                "pgqueuer_schedules": True,
            },
        ),
        (
            DurabilityPolicy(
                queue_table="UNLOGGED",
                queue_log_table="UNLOGGED",
                statistics_table="UNLOGGED",
                schedules_table="UNLOGGED",
            ),
            {
                "pgqueuer": False,
                "pgqueuer_log": False,
                "pgqueuer_statistics": False,
                "pgqueuer_schedules": False,
            },
        ),
    ],
)
def test_policy_attributes(policy: DurabilityPolicy, expected_logged: dict[str, bool]) -> None:
    """
    Test that individual attributes in `DurabilityPolicy` correctly indicate whether they
    are logged or unlogged.
    """
    assert (policy.queue_table == "") == expected_logged["pgqueuer"]
    assert (policy.queue_log_table == "") == expected_logged["pgqueuer_log"]
    assert (policy.statistics_table == "") == expected_logged["pgqueuer_statistics"]
    assert (policy.schedules_table == "") == expected_logged["pgqueuer_schedules"]


def test_invalid_durability_level() -> None:
    """
    Test that accessing an unknown durability level raises a ValueError.
    """
    with pytest.raises(ValueError, match="'nonexistent' is not a valid Durability"):
        Durability("nonexistent")


@asynccontextmanager
async def connection(dsn: str) -> AsyncIterator[asyncpg.Connection]:
    conn = await asyncpg.connect(dsn=dsn)
    try:
        yield conn
    finally:
        await conn.close()


async def installed_at(
    driver: AsyncpgDriver,
    settings: DBSettings,
    durability: Durability,
) -> Queries:
    queries = Queries(driver, settings)
    await queries.uninstall()
    await queries.install(durability=durability)
    return queries


def unlogged_at(settings: DBSettings, durability: Durability) -> dict[str, bool]:
    """Whether *durability* makes each PgQueuer table UNLOGGED."""
    policy = durability.config
    return {
        settings.queue_table: policy.queue_table == "UNLOGGED",
        settings.queue_table_log: policy.queue_log_table == "UNLOGGED",
        settings.statistics_table: policy.statistics_table == "UNLOGGED",
        settings.schedules_table: policy.schedules_table == "UNLOGGED",
    }


async def unlogged_now(queries: Queries) -> dict[str, bool]:
    return {info.table_name: info.persistence == "UNLOGGED" for info in await queries.schema_info()}


async def table_waited_on(
    watcher: asyncpg.Connection,
    pid: int,
    task: asyncio.Task[None],
) -> str | None:
    """Poll until backend *pid* waits for a table lock; None when *task* ends first."""
    async with asyncio.timeout(5):
        while not task.done():
            table = await watcher.fetchval(
                """SELECT relation::regclass::text FROM pg_locks
                WHERE pid = $1 AND locktype = 'relation' AND NOT granted""",
                pid,
            )
            if table is not None:
                return str(table)
            await asyncio.sleep(0.01)
    return None


@pytest.mark.parametrize(
    "settings",
    [
        pytest.param(DBSettings(), id="default"),
        pytest.param(DBSettings(prefix="acme_", db_schema="billing"), id="scoped"),
    ],
)
@pytest.mark.parametrize(
    ("start", "target"),
    [
        (Durability.durable, Durability.durable),
        (Durability.balanced, Durability.balanced),
        (Durability.volatile, Durability.volatile),
        (Durability.durable, Durability.balanced),
        (Durability.balanced, Durability.durable),
        (Durability.volatile, Durability.balanced),
        (Durability.balanced, Durability.volatile),
    ],
)
async def test_switch_never_locks_a_table_it_does_not_change(
    apgdriver: AsyncpgDriver,
    dsn: str,
    settings: DBSettings,
    start: Durability,
    target: Durability,
) -> None:
    """A durability switch takes no lock on a table that keeps its persistence."""
    queries = await installed_at(apgdriver, settings, start)
    unchanged = [
        settings.qualify(table)
        for table, unlogged in unlogged_at(settings, start).items()
        if unlogged_at(settings, target)[table] == unlogged
    ]
    await apgdriver.execute("SET lock_timeout = '1s'")

    async with connection(dsn) as blocker, blocker.transaction():
        await blocker.execute(f"LOCK TABLE {', '.join(unchanged)} IN ACCESS EXCLUSIVE MODE")
        await queries.alter_durability(target)

    assert await unlogged_now(queries) == unlogged_at(settings, target)


@pytest.mark.parametrize(
    ("start", "target"),
    [
        (Durability.durable, Durability.balanced),
        (Durability.balanced, Durability.durable),
        (Durability.durable, Durability.volatile),
        (Durability.volatile, Durability.durable),
    ],
)
async def test_queue_table_stays_usable_while_a_switch_waits_on_the_log_table(
    apgdriver: AsyncpgDriver,
    dsn: str,
    start: Durability,
    target: Durability,
) -> None:
    """A durability switch holds no lock on the queue table while it waits for the log table."""
    settings = DBSettings()
    log_table = settings.queue_table_log
    queries = await installed_at(apgdriver, settings, start)
    job_ids = await queries.enqueue("probe", None)
    rows = await apgdriver.fetch("SELECT pg_backend_pid() AS pid")
    switch_pid = cell(rows[0], "pid", int)

    async with connection(dsn) as blocker, connection(dsn) as worker:
        await worker.execute("SET lock_timeout = '1s'")
        worker_queries = Queries(AsyncpgDriver(worker), settings)
        async with asyncio.TaskGroup() as tasks, blocker.transaction():
            await blocker.execute(f"LOCK TABLE {log_table} IN ACCESS SHARE MODE")
            switch = tasks.create_task(queries.alter_durability(target))
            assert await table_waited_on(worker, switch_pid, switch) == log_table
            # A heartbeat writes only the queue table. Enqueue and dequeue also write the log.
            await worker_queries.update_heartbeat(job_ids)
            sizes = await worker_queries.queue_size()
            assert [(size.entrypoint, size.count) for size in sizes] == [("probe", 1)]

    assert await unlogged_now(queries) == unlogged_at(settings, target)
    assert [(log.job_id, log.status) for log in await queries.queue_log()] == [
        (job_ids[0], "queued")
    ]
