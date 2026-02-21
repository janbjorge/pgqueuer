"""Unit tests for InMemoryQueries — no QueueManager, direct API."""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.domain.errors import DuplicateJobError
from pgqueuer.ports.repository import EntrypointExecutionParameter


@pytest.fixture
def driver() -> InMemoryDriver:
    return InMemoryDriver()


@pytest.fixture
def queries(driver: InMemoryDriver) -> InMemoryQueries:
    return InMemoryQueries(driver=driver)


# ---------------------------------------------------------------------------
# Enqueue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_single(queries: InMemoryQueries) -> None:
    ids = await queries.enqueue("my_ep", b"payload", priority=5)
    assert len(ids) == 1
    assert isinstance(ids[0], int)

    stats = await queries.queue_size()
    assert len(stats) == 1
    assert stats[0].count == 1
    assert stats[0].entrypoint == "my_ep"
    assert stats[0].status == "queued"


@pytest.mark.asyncio
async def test_enqueue_batch(queries: InMemoryQueries) -> None:
    ids = await queries.enqueue(
        ["a", "b", "c"],
        [b"1", b"2", b"3"],
        [1, 2, 3],
    )
    assert len(ids) == 3
    assert ids[0] < ids[1] < ids[2]


@pytest.mark.asyncio
async def test_enqueue_returns_ids_with_priority(queries: InMemoryQueries) -> None:
    ids = await queries.enqueue("ep", None, priority=10)
    assert len(ids) == 1


# ---------------------------------------------------------------------------
# Dedupe
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dedupe_key_rejects_duplicate(queries: InMemoryQueries) -> None:
    await queries.enqueue("ep", None, dedupe_key="unique1")
    with pytest.raises(DuplicateJobError):
        await queries.enqueue("ep", None, dedupe_key="unique1")


@pytest.mark.asyncio
async def test_dedupe_key_freed_after_log(queries: InMemoryQueries) -> None:
    await queries.enqueue("ep", None, dedupe_key="dk1")
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )
    assert len(jobs) == 1
    await queries.log_jobs([(jobs[0], "successful", None)])

    # Should be able to enqueue again now
    ids2 = await queries.enqueue("ep", None, dedupe_key="dk1")
    assert len(ids2) == 1


# ---------------------------------------------------------------------------
# Dequeue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_dequeue_basic(queries: InMemoryQueries) -> None:
    await queries.enqueue("ep", b"data")
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )
    assert len(jobs) == 1
    assert jobs[0].entrypoint == "ep"
    assert jobs[0].status == "picked"
    assert jobs[0].payload == b"data"


@pytest.mark.asyncio
async def test_dequeue_priority_ordering(queries: InMemoryQueries) -> None:
    await queries.enqueue(
        ["ep", "ep", "ep"],
        [b"lo", b"hi", b"med"],
        [1, 10, 5],
    )
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )
    assert len(jobs) == 3
    priorities = [j.priority for j in jobs]
    assert priorities == sorted(priorities, reverse=True)


@pytest.mark.asyncio
async def test_dequeue_execute_after(queries: InMemoryQueries) -> None:
    await queries.enqueue("ep", None, execute_after=timedelta(hours=1))
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )
    # Job is in the future, shouldn't be dequeued
    assert len(jobs) == 0


@pytest.mark.asyncio
async def test_dequeue_serialized_dispatch(queries: InMemoryQueries) -> None:
    await queries.enqueue(
        ["ep", "ep"],
        [None, None],
        [0, 0],
    )
    qm_id = uuid.uuid4()
    params = {"ep": EntrypointExecutionParameter(timedelta(0), True, 0)}

    # First dequeue should get 1 job (serialized = only one at a time)
    jobs = await queries.dequeue(10, params, qm_id, None)
    assert len(jobs) == 1

    # Second dequeue should get 0 (first is still picked)
    jobs2 = await queries.dequeue(10, params, qm_id, None)
    assert len(jobs2) == 0


@pytest.mark.asyncio
async def test_dequeue_concurrency_limit(queries: InMemoryQueries) -> None:
    await queries.enqueue(
        ["ep", "ep", "ep"],
        [None, None, None],
        [0, 0, 0],
    )
    qm_id = uuid.uuid4()
    params = {"ep": EntrypointExecutionParameter(timedelta(0), False, 2)}

    jobs = await queries.dequeue(10, params, qm_id, None)
    assert len(jobs) == 2

    jobs2 = await queries.dequeue(10, params, qm_id, None)
    assert len(jobs2) == 0


@pytest.mark.asyncio
async def test_dequeue_global_concurrency_limit(queries: InMemoryQueries) -> None:
    await queries.enqueue(
        ["ep", "ep", "ep"],
        [None, None, None],
        [0, 0, 0],
    )
    qm_id = uuid.uuid4()
    params = {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)}

    jobs = await queries.dequeue(10, params, qm_id, global_concurrency_limit=1)
    assert len(jobs) == 1

    jobs2 = await queries.dequeue(10, params, qm_id, global_concurrency_limit=1)
    assert len(jobs2) == 0


@pytest.mark.asyncio
async def test_dequeue_retry_stale_picked(queries: InMemoryQueries) -> None:
    """Picked jobs with expired heartbeat should be retried."""
    await queries.enqueue("ep", None)
    qm_id = uuid.uuid4()
    retry_after = timedelta(seconds=1)
    params = {"ep": EntrypointExecutionParameter(retry_after, False, 0)}

    jobs = await queries.dequeue(10, params, qm_id, None)
    assert len(jobs) == 1

    # Manually make heartbeat old
    job_id = int(jobs[0].id)
    from datetime import datetime, timezone

    queries._jobs[job_id]["heartbeat"] = datetime(2000, 1, 1, tzinfo=timezone.utc)

    # Now dequeue again - should retry the stale job
    jobs2 = await queries.dequeue(10, params, qm_id, None)
    assert len(jobs2) == 1
    assert jobs2[0].id == jobs[0].id


@pytest.mark.asyncio
async def test_dequeue_empty_entrypoints(queries: InMemoryQueries) -> None:
    await queries.enqueue("ep", None)
    jobs = await queries.dequeue(10, {}, uuid.uuid4(), None)
    assert len(jobs) == 0


@pytest.mark.asyncio
async def test_dequeue_batch_size_validation(queries: InMemoryQueries) -> None:
    with pytest.raises(ValueError):
        await queries.dequeue(0, {}, uuid.uuid4(), None)


# ---------------------------------------------------------------------------
# log_jobs
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_log_jobs_removes_from_queue(queries: InMemoryQueries) -> None:
    await queries.enqueue("ep", None)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )
    await queries.log_jobs([(jobs[0], "successful", None)])

    stats = await queries.queue_size()
    assert len(stats) == 0


@pytest.mark.asyncio
async def test_log_jobs_adds_to_log(queries: InMemoryQueries) -> None:
    await queries.enqueue("ep", None)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )
    await queries.log_jobs([(jobs[0], "successful", None)])

    log = await queries.queue_log()
    statuses = [e.status for e in log]
    assert "successful" in statuses


# ---------------------------------------------------------------------------
# mark_job_as_cancelled
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_mark_job_as_cancelled(queries: InMemoryQueries, driver: InMemoryDriver) -> None:
    notifications: list[str] = []
    await driver.add_listener(queries.qbq.settings.channel, notifications.append)

    ids = await queries.enqueue("ep", None)
    await queries.mark_job_as_cancelled(ids)

    stats = await queries.queue_size()
    assert len(stats) == 0

    # Should have received cancellation notification
    assert any("cancellation_event" in n for n in notifications)


# ---------------------------------------------------------------------------
# clear_queue
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_clear_queue_truncate(queries: InMemoryQueries) -> None:
    await queries.enqueue(["a", "b"], [None, None], [0, 0])
    await queries.clear_queue()
    stats = await queries.queue_size()
    assert len(stats) == 0

    # Truncate should not create log entries
    log = await queries.queue_log()
    # Only the 'queued' entries from enqueue should exist
    deleted_entries = [e for e in log if e.status == "deleted"]
    assert len(deleted_entries) == 0


@pytest.mark.asyncio
async def test_clear_queue_filtered(queries: InMemoryQueries) -> None:
    await queries.enqueue(["a", "b"], [None, None], [0, 0])
    await queries.clear_queue("a")

    stats = await queries.queue_size()
    assert len(stats) == 1
    assert stats[0].entrypoint == "b"

    log = await queries.queue_log()
    deleted_entries = [e for e in log if e.status == "deleted"]
    assert len(deleted_entries) == 1
    assert deleted_entries[0].entrypoint == "a"


# ---------------------------------------------------------------------------
# update_heartbeat
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_update_heartbeat(queries: InMemoryQueries) -> None:
    await queries.enqueue("ep", None)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )
    old_hb = jobs[0].heartbeat
    await asyncio.sleep(0.01)
    await queries.update_heartbeat([jobs[0].id])
    new_hb = queries._jobs[int(jobs[0].id)]["heartbeat"]
    assert new_hb > old_hb


# ---------------------------------------------------------------------------
# queue_log lifecycle
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_queue_log_lifecycle(queries: InMemoryQueries) -> None:
    await queries.enqueue("ep", None)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )
    await queries.log_jobs([(jobs[0], "successful", None)])

    log = await queries.queue_log()
    statuses = [e.status for e in log if e.entrypoint == "ep"]
    assert "queued" in statuses
    assert "picked" in statuses
    assert "successful" in statuses


# ---------------------------------------------------------------------------
# log_statistics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_log_statistics(queries: InMemoryQueries) -> None:
    await queries.enqueue("ep", None)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )
    await queries.log_jobs([(jobs[0], "successful", None)])

    stats = await queries.log_statistics(tail=10)
    assert len(stats) > 0
    assert any(s.entrypoint == "ep" for s in stats)


# ---------------------------------------------------------------------------
# job_status
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_job_status(queries: InMemoryQueries) -> None:
    ids = await queries.enqueue("ep", None)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )
    await queries.log_jobs([(jobs[0], "successful", None)])

    result = await queries.job_status(ids)
    assert len(result) == 1
    assert result[0][1] == "successful"


# ---------------------------------------------------------------------------
# queued_work
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_queued_work(queries: InMemoryQueries) -> None:
    await queries.enqueue(["ep", "ep", "other"], [None, None, None], [0, 0, 0])
    count = await queries.queued_work(["ep"])
    assert count == 2


# ---------------------------------------------------------------------------
# Schedule CRUD
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schedule_lifecycle(queries: InMemoryQueries) -> None:
    from pgqueuer.domain.models import CronExpressionEntrypoint
    from pgqueuer.domain.types import CronEntrypoint, CronExpression

    key = CronExpressionEntrypoint(
        CronEntrypoint("my_cron"),
        CronExpression("* * * * *"),
    )

    await queries.insert_schedule({key: timedelta(seconds=0)})
    schedules = await queries.peak_schedule()
    assert len(schedules) == 1
    assert schedules[0].entrypoint == "my_cron"

    # Insert again should be no-op (ON CONFLICT DO NOTHING)
    await queries.insert_schedule({key: timedelta(seconds=0)})
    schedules = await queries.peak_schedule()
    assert len(schedules) == 1

    # Fetch schedule (should pick it since next_run <= now)
    fetched = await queries.fetch_schedule({key: timedelta(seconds=60)})
    assert len(fetched) == 1
    assert fetched[0].status == "picked"

    # Set back to queued
    await queries.set_schedule_queued({fetched[0].id})
    schedules = await queries.peak_schedule()
    assert schedules[0].status == "queued"

    # Delete by entrypoint
    await queries.delete_schedule(set(), {CronEntrypoint("my_cron")})
    schedules = await queries.peak_schedule()
    assert len(schedules) == 0


@pytest.mark.asyncio
async def test_clear_schedule(queries: InMemoryQueries) -> None:
    from pgqueuer.domain.models import CronExpressionEntrypoint
    from pgqueuer.domain.types import CronEntrypoint, CronExpression

    key = CronExpressionEntrypoint(
        CronEntrypoint("my_cron"),
        CronExpression("* * * * *"),
    )
    await queries.insert_schedule({key: timedelta(seconds=0)})
    await queries.clear_schedule()
    schedules = await queries.peak_schedule()
    assert len(schedules) == 0


# ---------------------------------------------------------------------------
# Notification delivery
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_table_changed_notification(queries: InMemoryQueries, driver: InMemoryDriver) -> None:
    notifications: list[str] = []
    await driver.add_listener(queries.qbq.settings.channel, notifications.append)

    await queries.enqueue("ep", None)
    assert any("table_changed_event" in n for n in notifications)


@pytest.mark.asyncio
async def test_rps_notification(queries: InMemoryQueries, driver: InMemoryDriver) -> None:
    notifications: list[str] = []
    await driver.add_listener(queries.qbq.settings.channel, notifications.append)

    await queries.notify_entrypoint_rps({"ep": 5})
    assert any("requests_per_second_event" in n for n in notifications)


@pytest.mark.asyncio
async def test_health_check_notification(queries: InMemoryQueries, driver: InMemoryDriver) -> None:
    notifications: list[str] = []
    await driver.add_listener(queries.qbq.settings.channel, notifications.append)

    await queries.notify_health_check(uuid.uuid4())
    assert any("health_check_event" in n for n in notifications)


# ---------------------------------------------------------------------------
# Schema methods
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schema_methods_return_true(queries: InMemoryQueries) -> None:
    assert await queries.has_table("any_table") is True
    assert await queries.table_has_column("t", "c") is True
    assert await queries.table_has_index("t", "i") is True
    assert await queries.has_user_defined_enum("k", "e") is True


@pytest.mark.asyncio
async def test_install_upgrade_noop(queries: InMemoryQueries) -> None:
    await queries.install()
    await queries.upgrade()


@pytest.mark.asyncio
async def test_uninstall_clears_state(queries: InMemoryQueries) -> None:
    await queries.enqueue("ep", None)
    await queries.uninstall()
    stats = await queries.queue_size()
    assert len(stats) == 0


# ---------------------------------------------------------------------------
# InMemoryDriver
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_driver_context_manager(driver: InMemoryDriver) -> None:
    async with driver as d:
        assert d is driver


@pytest.mark.asyncio
async def test_driver_fetch_raises(driver: InMemoryDriver) -> None:
    with pytest.raises(NotImplementedError):
        await driver.fetch("SELECT 1")


@pytest.mark.asyncio
async def test_driver_execute_raises(driver: InMemoryDriver) -> None:
    with pytest.raises(NotImplementedError):
        await driver.execute("SELECT 1")


def test_driver_shutdown_event(driver: InMemoryDriver) -> None:
    assert isinstance(driver.shutdown, asyncio.Event)
    assert not driver.shutdown.is_set()
