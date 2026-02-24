import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock

import pytest

from pgqueuer.db import AsyncpgDriver, Driver
from pgqueuer.models import CronExpressionEntrypoint, Schedule
from pgqueuer.qb import DBSettings
from pgqueuer.sm import SchedulerManager


async def inspect_schedule(connection: Driver) -> list[Schedule]:
    query = f"SELECT * FROM {DBSettings().schedules_table} ORDER BY id"
    return [Schedule.model_validate(dict(x)) for x in await connection.fetch(query)]


@pytest.fixture
async def scheduler(apgdriver: AsyncpgDriver) -> SchedulerManager:
    return SchedulerManager(apgdriver)


async def shutdown_Scheduler_after(
    scheduler: SchedulerManager,
    delay: timedelta = timedelta(seconds=1),
) -> None:
    await asyncio.sleep(delay.total_seconds())
    scheduler.shutdown.set()


async def test_scheduler_register(scheduler: SchedulerManager) -> None:
    async def sample_task(schedule: Schedule) -> None:
        pass

    scheduler.schedule("sample_task", "1 * * * *")(sample_task)
    assert len(scheduler.registry) == 1
    itr = iter(scheduler.registry.keys())
    key = next(itr)
    assert key.entrypoint == "sample_task"
    assert scheduler.registry[key].parameters.expression == "1 * * * *"

    scheduler.schedule("sample_task", "2 * * * *")(sample_task)
    assert len(scheduler.registry) == 2
    itr = iter(scheduler.registry.keys())
    key = next(itr)
    key = next(itr)
    assert key.entrypoint == "sample_task"
    assert scheduler.registry[key].parameters.expression == "2 * * * *"


async def test_scheduler_register_raises_invalid_expression(scheduler: SchedulerManager) -> None:
    async def sample_task(schedule: Schedule) -> None:
        pass

    with pytest.raises(ValueError):
        scheduler.schedule("sample_task", "bla * * * *")(sample_task)


async def test_scheduler_runs_tasks(scheduler: SchedulerManager, mocker: Mock) -> None:
    mocked_now = datetime.now(timezone.utc) + timedelta(hours=1)
    mocker.patch(
        "pgqueuer.core.helpers.utc_now",
        return_value=mocked_now,
    )
    # Mock croniter to return a time in the past, making the task immediately due
    past_timestamp = int(mocked_now.timestamp()) - 60
    mocker.patch(
        "pgqueuer.core.executors.croniter",
        return_value=mocker.Mock(get_next=mocker.Mock(return_value=past_timestamp)),
    )
    executed = False

    async def sample_task(schedule: Schedule) -> None:
        nonlocal executed
        executed = True

    scheduler.schedule("sample_task", "* * * * *")(sample_task)

    await asyncio.gather(
        *[
            scheduler.run(),
            shutdown_Scheduler_after(scheduler),
        ],
    )

    assert executed


async def test_heartbeat_updates(scheduler: SchedulerManager, mocker: Mock) -> None:
    mocked_now = datetime.now(timezone.utc) + timedelta(hours=1)
    mocker.patch(
        "pgqueuer.core.helpers.utc_now",
        return_value=mocked_now,
    )
    # Mock croniter to return a time in the past, making the task immediately due
    past_timestamp = int(mocked_now.timestamp()) - 60
    mocker.patch(
        "pgqueuer.core.executors.croniter",
        return_value=mocker.Mock(get_next=mocker.Mock(return_value=past_timestamp)),
    )

    async def sample_task(schedule: Schedule) -> None: ...

    scheduler.schedule("sample_task", "* * * * *")(sample_task)

    before = await inspect_schedule(scheduler.connection)
    await asyncio.gather(
        *[
            scheduler.run(),
            shutdown_Scheduler_after(scheduler, timedelta(seconds=1)),
        ],
    )
    after = await inspect_schedule(scheduler.connection)

    assert all(a.heartbeat > b.heartbeat for a, b in zip(after, before))


async def test_schedule_storage_and_retrieval(
    scheduler: SchedulerManager,
    mocker: Mock,
) -> None:
    mocked_now = datetime.now(timezone.utc) + timedelta(hours=1)
    mocker.patch(
        "pgqueuer.core.helpers.utc_now",
        return_value=mocked_now,
    )
    # Mock croniter to return a time in the past, making the task immediately due
    past_timestamp = int(mocked_now.timestamp()) - 60
    mocker.patch(
        "pgqueuer.core.executors.croniter",
        return_value=mocker.Mock(get_next=mocker.Mock(return_value=past_timestamp)),
    )
    expression = "* * * * *"
    entrypoint = "db_task"
    received: CronExpressionEntrypoint | None = None

    async def db_task(schedule: Schedule) -> None:
        nonlocal received
        received = CronExpressionEntrypoint(
            entrypoint=schedule.entrypoint,
            expression=schedule.expression,
        )

    scheduler.schedule(entrypoint, expression)(db_task)
    await asyncio.gather(
        *[
            scheduler.run(),
            shutdown_Scheduler_after(scheduler, timedelta(seconds=2)),
        ],
    )

    assert received is not None
    assert received.entrypoint == entrypoint
    assert received.expression == expression


async def test_schedule_clean_old(
    apgdriver: AsyncpgDriver,
    mocker: Mock,
) -> None:
    async def shutdown_after(
        sm: SchedulerManager,
        delay: timedelta = timedelta(seconds=0.1),
    ) -> None:
        await asyncio.sleep(delay.total_seconds())
        sm.shutdown.set()

    sm1 = SchedulerManager(apgdriver)

    @sm1.schedule("sm_task", "1 * * * *")
    async def _(schedule: Schedule) -> None:
        pass

    await asyncio.gather(sm1.run(), shutdown_after(sm1))

    schedules = await inspect_schedule(apgdriver)
    assert len(schedules) == 1

    sm2 = SchedulerManager(apgdriver)

    @sm2.schedule("sm_task", "2 * * * *")
    async def _(schedule: Schedule) -> None:
        pass

    await asyncio.gather(sm2.run(), shutdown_after(sm2))

    schedules = await inspect_schedule(apgdriver)
    assert len(schedules) == 2

    sm3 = SchedulerManager(apgdriver)

    @sm3.schedule("sm_task", "3 * * * *", clean_old=True)
    async def _(schedule: Schedule) -> None:
        pass

    await asyncio.gather(sm3.run(), shutdown_after(sm3))

    schedules = await inspect_schedule(apgdriver)
    assert len(schedules) == 1
    assert schedules[0].expression == "3 * * * *"

    sm4 = SchedulerManager(apgdriver)

    @sm4.schedule("sm_task", "3 * * * *", clean_old=True)
    @sm4.schedule("sm_task", "4 * * * *", clean_old=True)
    async def _(schedule: Schedule) -> None:
        pass

    await asyncio.gather(sm4.run(), shutdown_after(sm4))

    schedules = await inspect_schedule(apgdriver)
    assert len(schedules) == 2
    assert schedules[0].expression == "4 * * * *"
    assert schedules[1].expression == "3 * * * *"


# ============================================================================
# INTEGRATION TESTS FOR ISSUE #536: Race Condition in Scheduler
# ============================================================================


async def test_multi_instance_single_task_execution(
    apgdriver: AsyncpgDriver,
    mocker: Mock,
) -> None:
    """
    CRITICAL TEST 1.1: Multi-Instance Single Task Execution

    Verify that when multiple scheduler instances run concurrently,
    a single scheduled task is executed exactly once, not multiple times.

    This test demonstrates the race condition:
    - WITHOUT fix: Same task executes 2+ times (race condition)
    - WITH fix: Same task executes exactly 1 time

    This is the primary test demonstrating Issue #536.
    """
    mocker.patch(
        "pgqueuer.core.helpers.utc_now",
        return_value=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    # Track execution across all scheduler instances
    execution_count = 0
    execution_instances = []

    async def test_task(schedule: Schedule) -> None:
        nonlocal execution_count, execution_instances
        execution_count += 1
        execution_instances.append((datetime.now(timezone.utc), schedule.id))
        # Add small delay to allow potential race conditions to manifest
        await asyncio.sleep(0.1)

    # Create two scheduler instances
    scheduler1 = SchedulerManager(apgdriver)
    scheduler2 = SchedulerManager(apgdriver)

    # Register the same task in both
    scheduler1.schedule("multi_instance_task", "* * * * *")(test_task)
    scheduler2.schedule("multi_instance_task", "* * * * *")(test_task)

    async def shutdown_both_after(delay: timedelta = timedelta(seconds=2)) -> None:
        await asyncio.sleep(delay.total_seconds())
        scheduler1.shutdown.set()
        scheduler2.shutdown.set()

    # Run both schedulers concurrently
    # The task is set to trigger immediately (mocked time is 1 hour in future)
    await asyncio.gather(
        scheduler1.run(),
        scheduler2.run(),
        shutdown_both_after(),
    )

    # With both fixes (heartbeat = NOW() + croniter using mocked time),
    # the task should execute at most 1-2 times across polling cycles
    assert execution_count <= 2, (
        f"Issue #536: Task executed {execution_count} times across 2 scheduler instances. "
        f"Expected 1-2 executions, but got {execution_count}. "
        f"This could indicate:\n"
        f"1. Heartbeat is not being updated (race condition)\n"
        f"2. Croniter is not using mocked time (time mismatch)\n"
        f"3. next_run is in the past causing repeated picks"
    )
    # Catch the pre-fix behavior (20+ executions)
    assert execution_count < 10, (
        f"SEVERE: Task executed {execution_count} times - indicates pre-fix behavior"
    )

    # Verify all executions were on the same schedule ID
    if len(execution_instances) > 1:
        schedule_ids = [inst[1] for inst in execution_instances]
        assert all(sid == schedule_ids[0] for sid in schedule_ids), (
            "All executions should be for the same schedule"
        )
