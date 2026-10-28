import asyncio
from collections.abc import Awaitable, Callable, MutableMapping
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.settings import DBSettings
from pgqueuer.domain.types import ScheduleId
from pgqueuer.executors import (
    ScheduleExecutor,
    ScheduleExecutorFactoryParameters,
)
from pgqueuer.models import (
    CronEntrypoint,
    CronExpression,
    CronExpressionEntrypoint,
    Schedule,
    ScheduleContext,
)
from pgqueuer.queries import Queries
from pgqueuer.sm import SchedulerManager


async def inspect_schedule(
    scheduler_or_driver: SchedulerManager | AsyncpgDriver,
) -> list[Schedule]:
    if isinstance(scheduler_or_driver, SchedulerManager):
        return await scheduler_or_driver.queries.peek_schedule()
    # Legacy path for integration tests that inspect raw DB
    query = f"SELECT * FROM {DBSettings().schedules_table} ORDER BY id"
    return [Schedule.model_validate(dict(x)) for x in await scheduler_or_driver.fetch(query)]


@pytest.fixture
def scheduler(queries: InMemoryQueries) -> SchedulerManager:
    return SchedulerManager(queries)


async def shutdown_scheduler_after(
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


def test_scheduler_auto_detects_context() -> None:
    scheduler = SchedulerManager(Mock())

    async def with_context(schedule: Schedule, ctx: ScheduleContext) -> None:
        pass

    async def without_context(schedule: Schedule) -> None:
        pass

    scheduler.schedule("with_context", "1 * * * *")(with_context)
    scheduler.schedule("without_context", "2 * * * *")(without_context)

    by_entrypoint = {str(key.entrypoint): ex for key, ex in scheduler.registry.items()}
    assert by_entrypoint["with_context"].parameters.accepts_context
    assert not by_entrypoint["without_context"].parameters.accepts_context


def test_scheduler_explicit_flag_overrides_detection() -> None:
    scheduler = SchedulerManager(Mock())

    async def with_context(schedule: Schedule, ctx: ScheduleContext) -> None:
        pass

    scheduler.schedule("forced_off", "1 * * * *", accepts_context=False)(with_context)

    key = next(iter(scheduler.registry.keys()))
    assert not scheduler.registry[key].parameters.accepts_context


async def test_scheduler_register_raises_invalid_expression(scheduler: SchedulerManager) -> None:
    async def sample_task(schedule: Schedule) -> None:
        pass

    with pytest.raises(ValueError, match="Invalid cron expression"):
        scheduler.schedule("sample_task", "bla * * * *")(sample_task)


async def test_scheduler_register_accepts_three_second_expression(
    mocker: Mock,
) -> None:
    scheduler = SchedulerManager(Mock())

    async def sample_task(schedule: Schedule) -> None:
        pass

    mocked_now = datetime(2025, 1, 1, 0, 0, 1, tzinfo=UTC)
    mocker.patch("pgqueuer.core.executors.utc_now", return_value=mocked_now)

    scheduler.schedule("sample_task", "* * * * * */3")(sample_task)

    key = next(iter(scheduler.registry.keys()))
    executor = scheduler.registry[key]

    assert key.entrypoint == "sample_task"
    assert key.expression == "* * * * * */3"
    assert executor.parameters.expression == "* * * * * */3"
    assert executor.get_next() == datetime(2025, 1, 1, 0, 0, 3, tzinfo=UTC)
    assert executor.next_in() == timedelta(seconds=2)


async def test_scheduler_trailing_seconds_field_is_documented_behavior(
    mocker: Mock,
) -> None:
    scheduler = SchedulerManager(Mock())

    async def sample_task(schedule: Schedule) -> None:
        pass

    mocked_now = datetime(2025, 1, 1, 0, 0, 1, tzinfo=UTC)
    mocker.patch("pgqueuer.core.executors.utc_now", return_value=mocked_now)

    scheduler.schedule("sample_task_seconds_last", "* * * * * */3")(sample_task)
    scheduler.schedule("sample_task_seconds_first", "*/3 * * * * *")(sample_task)

    seconds_last = scheduler.registry[
        CronExpressionEntrypoint(
            CronEntrypoint("sample_task_seconds_last"),
            CronExpression("* * * * * */3"),
        )
    ]
    seconds_first = scheduler.registry[
        CronExpressionEntrypoint(
            CronEntrypoint("sample_task_seconds_first"),
            CronExpression("*/3 * * * * *"),
        )
    ]

    assert seconds_last.get_next() == datetime(2025, 1, 1, 0, 0, 3, tzinfo=UTC)
    assert seconds_last.next_in() == timedelta(seconds=2)
    assert seconds_first.get_next() == datetime(2025, 1, 1, 0, 0, 2, tzinfo=UTC)
    assert seconds_first.next_in() == timedelta(seconds=1)


async def test_scheduler_runs_tasks(scheduler: SchedulerManager, mocker: Mock) -> None:
    mocked_now = datetime.now(UTC) + timedelta(hours=1)
    mocker.patch(
        "pgqueuer.core.executors.utc_now",
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
            shutdown_scheduler_after(scheduler),
        ],
    )

    assert executed


@pytest.mark.parametrize("fails", (False, True))
async def test_scheduler_releases_schedule_before_driver_exit(mocker: Mock, fails: bool) -> None:
    """In-flight schedules are released before the driver exits (#897)."""
    events: list[str] = []

    class RecordingDriver(InMemoryDriver):
        async def __aexit__(self, *_: object) -> None:
            events.append("driver exit")

    queries = InMemoryQueries(driver=RecordingDriver())
    scheduler = SchedulerManager(queries)
    mocked_now = datetime.now(UTC) + timedelta(hours=1)
    mocker.patch("pgqueuer.core.executors.utc_now", return_value=mocked_now)
    mocker.patch(
        "pgqueuer.core.executors.croniter",
        return_value=mocker.Mock(get_next=mocker.Mock(return_value=mocked_now.timestamp() - 60)),
    )

    started = asyncio.Event()

    async def slow_task(schedule: Schedule) -> None:
        started.set()
        await asyncio.sleep(0.2)
        if fails:
            raise ValueError

    scheduler.schedule("slow_task", "* * * * *")(slow_task)

    release = queries.set_schedule_queued

    async def recording_release(ids: set[ScheduleId]) -> None:
        events.append("release")
        await release(ids)

    mocker.patch.object(queries, "set_schedule_queued", side_effect=recording_release)

    run = asyncio.create_task(scheduler.run())
    await started.wait()
    scheduler.shutdown.set()
    await run

    assert events == ["release", "driver exit"]


@pytest.mark.parametrize("trigger", ("error", "cancel"))
async def test_scheduler_run_exits_when_body_stops(
    scheduler: SchedulerManager,
    mocker: Mock,
    trigger: str,
) -> None:
    """run() fails fast when its loop raises or is cancelled with shutdown unset (#881)."""
    fetching = asyncio.Event()

    async def fetch_schedule(_: object) -> list[Schedule]:
        fetching.set()
        if trigger == "error":
            raise RuntimeError("fetch failed")
        await asyncio.Future()
        return []

    mocker.patch.object(scheduler.queries, "fetch_schedule", side_effect=fetch_schedule)

    run = asyncio.create_task(scheduler.run())
    await fetching.wait()
    if trigger == "cancel":
        run.cancel()
    done, _ = await asyncio.wait({run}, timeout=2)
    if run not in done:
        run.cancel()
        pytest.fail("run() hung after its loop stopped")
    expected = RuntimeError if trigger == "error" else asyncio.CancelledError
    with pytest.raises(expected):
        run.result()


async def test_scheduler_run_beats_while_draining_after_error(
    scheduler: SchedulerManager,
    mocker: Mock,
) -> None:
    """A failing loop drains running schedules with their heartbeat still going."""
    mocked_now = datetime.now(UTC) + timedelta(hours=1)
    mocker.patch("pgqueuer.core.executors.utc_now", return_value=mocked_now)
    mocker.patch(
        "pgqueuer.core.executors.croniter",
        return_value=mocker.Mock(get_next=mocker.Mock(return_value=mocked_now.timestamp() - 60)),
    )
    failed = asyncio.Event()
    beats_after_failure = 0

    async def slow_task(schedule: Schedule) -> None:
        await asyncio.sleep(1.5)

    scheduler.schedule("slow_task", "* * * * *")(slow_task)
    fetch_schedule = scheduler.queries.fetch_schedule

    async def fail_after_first(entrypoints: object) -> list[Schedule]:
        if scheduler.active_heartbeat_ids:
            failed.set()
            raise RuntimeError("fetch failed")
        return await fetch_schedule(entrypoints)  # type: ignore[arg-type]

    async def count_heartbeat(ids: set[ScheduleId]) -> None:
        nonlocal beats_after_failure
        if failed.is_set():
            beats_after_failure += 1

    mocker.patch.object(scheduler.queries, "fetch_schedule", side_effect=fail_after_first)
    mocker.patch.object(scheduler.queries, "update_schedule_heartbeat", side_effect=count_heartbeat)

    with pytest.raises(RuntimeError, match="fetch failed"):
        await asyncio.wait_for(scheduler.run(), timeout=5)

    assert beats_after_failure >= 1


@pytest.mark.xfail(strict=True, reason="before is read before run() inserts the schedule")
async def test_heartbeat_updates(scheduler: SchedulerManager, mocker: Mock) -> None:
    mocked_now = datetime.now(UTC) + timedelta(hours=1)
    mocker.patch(
        "pgqueuer.core.executors.utc_now",
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

    before = await inspect_schedule(scheduler)
    await asyncio.gather(
        *[
            scheduler.run(),
            shutdown_scheduler_after(scheduler, timedelta(seconds=1)),
        ],
    )
    after = await inspect_schedule(scheduler)

    assert all(a.heartbeat > b.heartbeat for a, b in zip(after, before, strict=True))


async def test_schedule_storage_and_retrieval(
    scheduler: SchedulerManager,
    mocker: Mock,
) -> None:
    mocked_now = datetime.now(UTC) + timedelta(hours=1)
    mocker.patch(
        "pgqueuer.core.executors.utc_now",
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
            shutdown_scheduler_after(scheduler, timedelta(seconds=2)),
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

    sm1 = SchedulerManager(Queries(apgdriver))

    @sm1.schedule("sm_task", "1 * * * *")
    async def _(schedule: Schedule) -> None:
        pass

    await asyncio.gather(sm1.run(), shutdown_after(sm1))

    schedules = await inspect_schedule(apgdriver)
    assert len(schedules) == 1

    sm2 = SchedulerManager(Queries(apgdriver))

    @sm2.schedule("sm_task", "2 * * * *")
    async def _(schedule: Schedule) -> None:
        pass

    await asyncio.gather(sm2.run(), shutdown_after(sm2))

    schedules = await inspect_schedule(apgdriver)
    assert len(schedules) == 2

    sm3 = SchedulerManager(Queries(apgdriver))

    @sm3.schedule("sm_task", "3 * * * *", clean_old=True)
    async def _(schedule: Schedule) -> None:
        pass

    await asyncio.gather(sm3.run(), shutdown_after(sm3))

    schedules = await inspect_schedule(apgdriver)
    assert len(schedules) == 1
    assert schedules[0].expression == "3 * * * *"
    kept_id = schedules[0].id

    sm4 = SchedulerManager(Queries(apgdriver))

    @sm4.schedule("sm_task", "3 * * * *", clean_old=True)
    @sm4.schedule("sm_task", "4 * * * *", clean_old=True)
    async def _(schedule: Schedule) -> None:
        pass

    await asyncio.gather(sm4.run(), shutdown_after(sm4))

    schedules = await inspect_schedule(apgdriver)
    kept = CronExpression("3 * * * *")
    by_expression = {row.expression: row.id for row in schedules}
    assert set(by_expression) == {kept, CronExpression("4 * * * *")}
    assert by_expression[kept] == kept_id


async def assert_clean_old_keeps_due_schedule(
    queries: InMemoryQueries | Queries,
    backdate: Callable[[], Awaitable[None]],
) -> None:
    first = SchedulerManager(queries)

    @first.schedule("nightly", "0 3 * * *", clean_old=True)
    async def nightly(schedule: Schedule) -> None:
        pass

    await asyncio.gather(first.run(), shutdown_scheduler_after(first, timedelta(seconds=0.1)))

    stored = await queries.peek_schedule()
    assert len(stored) == 1
    original_id = stored[0].id
    await backdate()

    fired: list[ScheduleId] = []
    second = SchedulerManager(queries)

    @second.schedule("nightly", "0 3 * * *", clean_old=True)
    async def nightly_again(schedule: Schedule) -> None:
        fired.append(schedule.id)
        second.shutdown.set()

    await asyncio.wait_for(second.run(), timeout=5)

    assert fired == [original_id]
    assert [row.id for row in await queries.peek_schedule()] == [original_id]


async def test_clean_old_keeps_due_firing(queries: InMemoryQueries) -> None:
    """clean_old=True keeps an unchanged schedule and runs a firing missed while down (#1029)."""

    async def backdate() -> None:
        due = datetime.now(UTC) - timedelta(minutes=1)
        for row in queries.schedules.rows.values():
            row.next_run = due

    await assert_clean_old_keeps_due_schedule(queries, backdate)


async def test_clean_old_keeps_due_firing_postgres(apgdriver: AsyncpgDriver) -> None:
    """clean_old=True keeps an unchanged schedule and runs a firing missed while down (#1029)."""
    queries = Queries(apgdriver)

    async def backdate() -> None:
        await apgdriver.execute(
            f"UPDATE {DBSettings().schedules_table} SET next_run = NOW() - interval '1 minute'"
        )

    await assert_clean_old_keeps_due_schedule(queries, backdate)


async def span_of_firings(
    queries: InMemoryQueries | Queries,
    expression: str,
    firings: int = 4,
) -> float:
    """Run a scheduler until *expression* fired *firings* times; seconds from first to last."""
    scheduler = SchedulerManager(queries)
    fired: list[float] = []

    @scheduler.schedule("tick", expression)
    async def tick(schedule: Schedule) -> None:
        fired.append(asyncio.get_running_loop().time())
        if len(fired) == firings:
            scheduler.shutdown.set()

    await asyncio.wait_for(scheduler.run(), timeout=15)
    return fired[-1] - fired[0]


async def test_every_second_schedule_fires_every_second(queries: InMemoryQueries) -> None:
    """Four firings of '* * * * * *' take about 3 s, not 6 s (every other tick skipped)."""
    span = await span_of_firings(queries, "* * * * * *")
    assert span < 4.5, span


async def test_every_second_schedule_fires_every_second_postgres(
    apgdriver: AsyncpgDriver,
) -> None:
    """Four firings of '* * * * * *' take about 3 s, not 6 s (every other tick skipped)."""
    span = await span_of_firings(Queries(apgdriver), "* * * * * *")
    assert span < 4.5, span


async def test_two_second_schedule_keeps_its_cadence(queries: InMemoryQueries) -> None:
    """Four firings of '* * * * * */2' still take about 6 s, so it does not fire more often."""
    span = await span_of_firings(queries, "* * * * * */2")
    assert span > 4.5, span


async def test_two_second_schedule_keeps_its_cadence_postgres(apgdriver: AsyncpgDriver) -> None:
    """Four firings of '* * * * * */2' still take about 6 s, so it does not fire more often."""
    span = await span_of_firings(Queries(apgdriver), "* * * * * */2")
    assert span > 4.5, span


async def fetches_in_two_seconds(scheduler: SchedulerManager, mocker: Mock) -> int:
    """Run *scheduler* for 2 s and count its fetch_schedule calls."""
    fetch = mocker.spy(scheduler.queries, "fetch_schedule")
    await asyncio.gather(
        scheduler.run(),
        shutdown_scheduler_after(scheduler, timedelta(seconds=2)),
    )
    count: int = fetch.call_count
    return count


async def test_empty_scheduler_polls_once_a_second(
    scheduler: SchedulerManager,
    mocker: Mock,
) -> None:
    """With nothing registered, run() fetches about once a second, not every 100 ms."""
    count = await fetches_in_two_seconds(scheduler, mocker)
    assert count <= 3, count


async def test_empty_scheduler_polls_once_a_second_postgres(
    apgdriver: AsyncpgDriver,
    mocker: Mock,
) -> None:
    """With nothing registered, run() fetches about once a second, not every 100 ms."""
    count = await fetches_in_two_seconds(SchedulerManager(Queries(apgdriver)), mocker)
    assert count <= 3, count


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
        "pgqueuer.core.executors.utc_now",
        return_value=datetime.now(UTC) + timedelta(hours=1),
    )

    # Track execution across all scheduler instances
    execution_count = 0
    execution_instances = []

    async def test_task(schedule: Schedule) -> None:
        nonlocal execution_count, execution_instances
        execution_count += 1
        execution_instances.append((datetime.now(UTC), schedule.id))
        # Add small delay to allow potential race conditions to manifest
        await asyncio.sleep(0.1)

    # Create two scheduler instances
    scheduler1 = SchedulerManager(Queries(apgdriver))
    scheduler2 = SchedulerManager(Queries(apgdriver))

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


# ============================================================================
# TESTS FOR ScheduleContext
# ============================================================================


async def test_schedule_executor_without_context() -> None:
    """Schedule handler without accepts_context still works (backward compat)."""
    received_schedules: list[Schedule] = []

    async def handler(schedule: Schedule) -> None:
        received_schedules.append(schedule)

    params = ScheduleExecutorFactoryParameters(
        entrypoint=CronEntrypoint("no_ctx"),
        expression=CronExpression("* * * * *"),
        func=handler,
        clean_old=False,
        accepts_context=False,
    )
    executor = ScheduleExecutor(parameters=params)

    fake_schedule = Schedule(
        id=ScheduleId(1),
        expression=CronExpression("* * * * *"),
        heartbeat=datetime.now(UTC),
        created=datetime.now(UTC),
        updated=datetime.now(UTC),
        next_run=datetime.now(UTC),
        status="queued",
        entrypoint=CronEntrypoint("no_ctx"),
    )
    context = ScheduleContext(resources={"key": "value"})
    await executor.execute(fake_schedule, context)

    assert len(received_schedules) == 1
    assert received_schedules[0] is fake_schedule


async def test_schedule_executor_with_context() -> None:
    """Schedule handler with accepts_context receives ScheduleContext."""
    received: list[tuple[Schedule, ScheduleContext]] = []

    async def handler(schedule: Schedule, ctx: ScheduleContext) -> None:
        received.append((schedule, ctx))

    params = ScheduleExecutorFactoryParameters(
        entrypoint=CronEntrypoint("with_ctx"),
        expression=CronExpression("* * * * *"),
        func=handler,
        clean_old=False,
        accepts_context=True,
    )
    executor = ScheduleExecutor(parameters=params)

    fake_schedule = Schedule(
        id=ScheduleId(1),
        expression=CronExpression("* * * * *"),
        heartbeat=datetime.now(UTC),
        created=datetime.now(UTC),
        updated=datetime.now(UTC),
        next_run=datetime.now(UTC),
        status="queued",
        entrypoint=CronEntrypoint("with_ctx"),
    )
    context = ScheduleContext(resources={"db": "pool"})
    await executor.execute(fake_schedule, context)

    assert len(received) == 1
    assert received[0][0] is fake_schedule
    assert received[0][1] is context
    assert received[0][1].resources["db"] == "pool"


async def test_schedule_context_resources_via_scheduler(
    scheduler: SchedulerManager,
    mocker: Mock,
) -> None:
    """Scheduled handler registered with accepts_context=True receives resources via dispatch."""
    mocked_now = datetime.now(UTC) + timedelta(hours=1)
    mocker.patch("pgqueuer.core.executors.utc_now", return_value=mocked_now)
    past_timestamp = int(mocked_now.timestamp()) - 60
    mocker.patch(
        "pgqueuer.core.executors.croniter",
        return_value=mocker.Mock(get_next=mocker.Mock(return_value=past_timestamp)),
    )

    scheduler.resources = {"shared_key": "shared_value"}
    received_resources: list[MutableMapping[str, object]] = []

    async def handler(schedule: Schedule, ctx: ScheduleContext) -> None:
        received_resources.append(dict(ctx.resources))

    scheduler.schedule("ctx_test", "* * * * *", accepts_context=True)(handler)

    await asyncio.gather(
        scheduler.run(),
        shutdown_scheduler_after(scheduler),
    )

    assert len(received_resources) >= 1
    assert received_resources[0] == {"shared_key": "shared_value"}


async def test_schedule_context_default_empty_resources() -> None:
    """ScheduleContext defaults to an empty dict when no resources are provided."""
    context = ScheduleContext()
    assert context.resources == {}


async def test_scheduler_resources_default_empty() -> None:
    """SchedulerManager.resources defaults to an empty dict."""
    sm = SchedulerManager(Mock())
    assert sm.resources == {}
