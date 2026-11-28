from __future__ import annotations

import asyncio

import asyncpg
import pytest

from pgqueuer.db import AsyncpgDriver, AsyncpgPoolDriver
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


async def test_switch_does_not_lock_a_table_already_at_the_target(
    apgdriver: AsyncpgDriver,
    dsn: str,
) -> None:
    settings = DBSettings()
    queries = Queries(apgdriver)
    await apgdriver.execute("SET lock_timeout = '1s'")
    async with (
        asyncpg.create_pool(dsn=dsn, min_size=1, max_size=1) as pool,
        pool.acquire() as blocker,
        blocker.transaction(),
    ):
        await blocker.execute(f"LOCK TABLE {settings.queue_table} IN ACCESS EXCLUSIVE MODE")
        await queries.alter_durability(Durability.balanced)

    unlogged = {
        info.table_name for info in await queries.schema_info() if info.persistence == "UNLOGGED"
    }
    assert unlogged == {settings.queue_table_log, settings.statistics_table}


async def test_heartbeat_does_not_wait_while_a_switch_waits_for_the_log_table(
    apgdriver: AsyncpgDriver,
    dsn: str,
) -> None:
    log_table = DBSettings().queue_table_log
    queries = Queries(apgdriver)
    job_ids = await queries.enqueue("probe", None)
    await apgdriver.execute("SET lock_timeout = '1s'")
    async with (
        asyncpg.create_pool(dsn=dsn, min_size=2, max_size=2) as pool,
        pool.acquire() as blocker,
        asyncio.TaskGroup() as tasks,
        blocker.transaction(),
    ):
        await blocker.execute(f"LOCK TABLE {log_table} IN ACCESS SHARE MODE")
        _ = tasks.create_task(
            Queries(AsyncpgPoolDriver(pool)).alter_durability(Durability.volatile)
        )
        async with asyncio.timeout(5):
            while not await blocker.fetchval(
                """SELECT EXISTS (SELECT FROM pg_locks
                WHERE relation = to_regclass($1) AND NOT granted)""",
                log_table,
            ):
                await asyncio.sleep(0.01)
        # A heartbeat writes only the queue table.
        await queries.update_heartbeat(job_ids)
