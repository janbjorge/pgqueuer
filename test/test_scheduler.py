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


@pytest.mark.asyncio
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
