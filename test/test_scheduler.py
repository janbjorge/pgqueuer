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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
async def test_scheduler_register_raises_invalid_expression(scheduler: SchedulerManager) -> None:
    async def sample_task(schedule: Schedule) -> None:
        pass

    with pytest.raises(ValueError):
        scheduler.schedule("sample_task", "bla * * * *")(sample_task)


@pytest.mark.asyncio
async def test_scheduler_runs_tasks(scheduler: SchedulerManager, mocker: Mock) -> None:
    mocker.patch(
        "pgqueuer.helpers.utc_now",
        return_value=datetime.now(timezone.utc) + timedelta(hours=1),
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


@pytest.mark.asyncio
async def test_heartbeat_updates(scheduler: SchedulerManager, mocker: Mock) -> None:
    mocker.patch(
        "pgqueuer.helpers.utc_now",
        return_value=datetime.now(timezone.utc) + timedelta(hours=1),
    )

    async def sample_task(schedule: Schedule) -> None: ...

    scheduler.schedule("sample_task", "* * * * *")(sample_task)

    before = await inspect_schedule(scheduler.connection)
    await asyncio.gather(
        *[
            scheduler.run(),
            shutdown_Scheduler_after(scheduler, timedelta(seconds=2)),
        ],
    )
    after = await inspect_schedule(scheduler.connection)

    assert all(a.heartbeat > b.heartbeat for a, b in zip(after, before))


@pytest.mark.asyncio
async def test_schedule_storage_and_retrieval(
    scheduler: SchedulerManager,
    mocker: Mock,
) -> None:
    mocker.patch(
        "pgqueuer.helpers.utc_now",
        return_value=datetime.now(timezone.utc) + timedelta(hours=1),
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
