"""Unit tests for the in-memory repository adapter."""

from __future__ import annotations

import asyncio
import uuid
from datetime import timedelta

import pytest

from pgqueuer.adapters.in_memory.driver import InMemoryDriver
from pgqueuer.adapters.in_memory.repository import InMemoryRepository
from pgqueuer.adapters.persistence.queries import EntrypointExecutionParameter
from pgqueuer.domain import errors, models
from pgqueuer.domain.types import CronEntrypoint, CronExpression, JobId


def _make_pair() -> tuple[InMemoryDriver, InMemoryRepository]:
    driver = InMemoryDriver()
    repo = InMemoryRepository(_driver=driver)
    return driver, repo


def _ep_params(
    retry_after: timedelta = timedelta(seconds=0),
    serialized: bool = False,
    concurrency_limit: int = 0,
) -> EntrypointExecutionParameter:
    return EntrypointExecutionParameter(
        retry_after=retry_after,
        serialized=serialized,
        concurrency_limit=concurrency_limit,
    )


QM_ID = uuid.uuid4()


# -----------------------------------------------------------------------
# Enqueue / Dequeue basics
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_enqueue_single_and_dequeue() -> None:
    _, repo = _make_pair()
    ids = await repo.enqueue("task_a", b"data", priority=1)
    assert len(ids) == 1

    jobs = await repo.dequeue(10, {"task_a": _ep_params()}, QM_ID, None)
    assert len(jobs) == 1
    assert jobs[0].entrypoint == "task_a"
    assert jobs[0].payload == b"data"
    assert jobs[0].status == "picked"


@pytest.mark.anyio
async def test_enqueue_batch() -> None:
    _, repo = _make_pair()
    ids = await repo.enqueue(
        ["a", "b", "c"],
        [b"1", b"2", b"3"],
        [1, 2, 3],
    )
    assert len(ids) == 3


@pytest.mark.anyio
async def test_dequeue_respects_priority() -> None:
    _, repo = _make_pair()
    await repo.enqueue("task", b"low", priority=0)
    await repo.enqueue("task", b"high", priority=10)

    jobs = await repo.dequeue(2, {"task": _ep_params()}, QM_ID, None)
    assert len(jobs) == 2
    assert jobs[0].priority == 10
    assert jobs[1].priority == 0


@pytest.mark.anyio
async def test_dequeue_respects_execute_after() -> None:
    _, repo = _make_pair()
    await repo.enqueue("task", b"later", execute_after=timedelta(hours=1))
    await repo.enqueue("task", b"now")

    jobs = await repo.dequeue(10, {"task": _ep_params()}, QM_ID, None)
    assert len(jobs) == 1
    assert jobs[0].payload == b"now"


# -----------------------------------------------------------------------
# Serialized dispatch
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_dequeue_serialized() -> None:
    _, repo = _make_pair()
    await repo.enqueue("task", b"1")
    await repo.enqueue("task", b"2")

    params = _ep_params(serialized=True)
    jobs = await repo.dequeue(10, {"task": params}, QM_ID, None)
    assert len(jobs) == 1  # Only one at a time.


# -----------------------------------------------------------------------
# Concurrency limits
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_dequeue_concurrency_limit() -> None:
    _, repo = _make_pair()
    for i in range(5):
        await repo.enqueue("task", f"payload_{i}".encode())

    params = _ep_params(concurrency_limit=2)
    jobs = await repo.dequeue(10, {"task": params}, QM_ID, None)
    assert len(jobs) == 2


@pytest.mark.anyio
async def test_dequeue_global_concurrency_limit() -> None:
    _, repo = _make_pair()
    for i in range(5):
        await repo.enqueue("task", f"payload_{i}".encode())

    jobs = await repo.dequeue(10, {"task": _ep_params()}, QM_ID, global_concurrency_limit=3)
    assert len(jobs) == 3


# -----------------------------------------------------------------------
# Retry of stale picked jobs
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_retry_stale_picked_jobs() -> None:
    _, repo = _make_pair()
    await repo.enqueue("task", b"data")

    # Pick the job.
    qm1 = uuid.uuid4()
    jobs = await repo.dequeue(1, {"task": _ep_params()}, qm1, None)
    assert len(jobs) == 1

    # Simulate stale heartbeat.
    jid = int(jobs[0].id)
    from datetime import datetime, timezone

    repo._jobs[jid]["heartbeat"] = datetime(2000, 1, 1, tzinfo=timezone.utc)

    # Retry with a different queue manager.
    qm2 = uuid.uuid4()
    params = _ep_params(retry_after=timedelta(seconds=1))
    retried = await repo.dequeue(1, {"task": params}, qm2, None)
    assert len(retried) == 1
    assert retried[0].id == jobs[0].id


# -----------------------------------------------------------------------
# log_jobs
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_log_jobs_removes_from_queue() -> None:
    _, repo = _make_pair()
    await repo.enqueue("task", b"data")
    jobs = await repo.dequeue(1, {"task": _ep_params()}, QM_ID, None)

    await repo.log_jobs([(jobs[0], "successful", None)])
    assert int(jobs[0].id) not in repo._jobs

    log = await repo.queue_log()
    statuses = [entry.status for entry in log if entry.job_id == jobs[0].id]
    assert "successful" in statuses


# -----------------------------------------------------------------------
# Dedupe key
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_dedupe_key_raises_duplicate_job_error() -> None:
    _, repo = _make_pair()
    await repo.enqueue("task", b"data", dedupe_key="unique-1")

    with pytest.raises(errors.DuplicateJobError):
        await repo.enqueue("task", b"data2", dedupe_key="unique-1")


@pytest.mark.anyio
async def test_dedupe_key_freed_after_log() -> None:
    _, repo = _make_pair()
    await repo.enqueue("task", b"data", dedupe_key="unique-1")
    jobs = await repo.dequeue(1, {"task": _ep_params()}, QM_ID, None)
    await repo.log_jobs([(jobs[0], "successful", None)])

    # Should not raise now.
    await repo.enqueue("task", b"data2", dedupe_key="unique-1")


# -----------------------------------------------------------------------
# mark_job_as_cancelled
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_mark_job_as_cancelled() -> None:
    driver, repo = _make_pair()

    notifications: list[str] = []
    await driver.add_listener(repo.qbe.settings.channel, lambda p: notifications.append(str(p)))

    ids = await repo.enqueue("task", b"data")
    await repo.mark_job_as_cancelled(ids)

    assert int(ids[0]) not in repo._jobs
    # Should have cancellation notification.
    assert any("cancellation_event" in n for n in notifications)


# -----------------------------------------------------------------------
# clear_queue
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_clear_queue_by_entrypoint() -> None:
    _, repo = _make_pair()
    await repo.enqueue("keep", b"data")
    await repo.enqueue("remove", b"data")

    await repo.clear_queue("remove")
    size = await repo.queue_size()
    entrypoints = {s.entrypoint for s in size}
    assert "keep" in entrypoints
    assert "remove" not in entrypoints


@pytest.mark.anyio
async def test_clear_queue_all() -> None:
    _, repo = _make_pair()
    await repo.enqueue("a", b"1")
    await repo.enqueue("b", b"2")

    await repo.clear_queue()
    size = await repo.queue_size()
    assert len(size) == 0


# -----------------------------------------------------------------------
# queue_size
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_queue_size_statistics() -> None:
    _, repo = _make_pair()
    await repo.enqueue(
        ["task", "task", "other"],
        [b"1", b"2", b"3"],
        [1, 1, 2],
    )
    size = await repo.queue_size()
    assert len(size) >= 2
    total = sum(s.count for s in size)
    assert total == 3


# -----------------------------------------------------------------------
# Notification events reach driver listeners
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_notification_events_reach_listeners() -> None:
    driver, repo = _make_pair()

    received: list[str] = []
    await driver.add_listener(repo.qbe.settings.channel, lambda p: received.append(str(p)))

    await repo.enqueue("task", b"data")
    assert len(received) >= 1
    assert "table_changed_event" in received[0]


# -----------------------------------------------------------------------
# Schedule CRUD
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_schedule_insert_and_fetch() -> None:
    _, repo = _make_pair()
    cee = models.CronExpressionEntrypoint(
        entrypoint=CronEntrypoint("my_cron"),
        expression=CronExpression("* * * * *"),
    )
    await repo.insert_schedule({cee: timedelta(seconds=0)})

    schedules = await repo.fetch_schedule({cee: timedelta(minutes=1)})
    assert len(schedules) == 1
    assert schedules[0].entrypoint == "my_cron"


@pytest.mark.anyio
async def test_schedule_set_queued() -> None:
    _, repo = _make_pair()
    cee = models.CronExpressionEntrypoint(
        entrypoint=CronEntrypoint("cron_task"),
        expression=CronExpression("* * * * *"),
    )
    await repo.insert_schedule({cee: timedelta(seconds=0)})
    fetched = await repo.fetch_schedule({cee: timedelta(minutes=1)})
    assert fetched[0].status == "picked"

    await repo.set_schedule_queued({fetched[0].id})
    peeked = await repo.peak_schedule()
    assert peeked[0].status == "queued"


@pytest.mark.anyio
async def test_schedule_delete() -> None:
    _, repo = _make_pair()
    cee = models.CronExpressionEntrypoint(
        entrypoint=CronEntrypoint("cron_del"),
        expression=CronExpression("0 * * * *"),
    )
    await repo.insert_schedule({cee: timedelta(seconds=0)})
    await repo.delete_schedule(set(), {CronEntrypoint("cron_del")})
    peeked = await repo.peak_schedule()
    assert len(peeked) == 0


@pytest.mark.anyio
async def test_clear_schedule() -> None:
    _, repo = _make_pair()
    cee = models.CronExpressionEntrypoint(
        entrypoint=CronEntrypoint("cron_clear"),
        expression=CronExpression("0 0 * * *"),
    )
    await repo.insert_schedule({cee: timedelta(seconds=0)})
    await repo.clear_schedule()
    assert len(repo._schedules) == 0


# -----------------------------------------------------------------------
# Schema management no-ops
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_schema_management_noop() -> None:
    _, repo = _make_pair()
    await repo.install()
    await repo.upgrade()
    assert await repo.has_table("anything") is True
    assert await repo.table_has_column("t", "c") is True
    assert await repo.table_has_index("t", "i") is True
    assert await repo.has_user_defined_enum("k", "e") is True
    await repo.uninstall()
    assert len(repo._jobs) == 0


# -----------------------------------------------------------------------
# Driver protocol
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_driver_protocol() -> None:
    driver = InMemoryDriver()
    assert await driver.fetch("SELECT 1") == []
    assert await driver.execute("SELECT 1") == ""
    assert not driver.shutdown.is_set()
    assert driver.tm is not None

    async with driver as d:
        assert d is driver


# -----------------------------------------------------------------------
# queued_work
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_queued_work() -> None:
    _, repo = _make_pair()
    await repo.enqueue("task", b"1")
    await repo.enqueue("task", b"2")
    await repo.enqueue("other", b"3")

    count = await repo.queued_work(["task"])
    assert count == 2


# -----------------------------------------------------------------------
# log_statistics
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_log_statistics() -> None:
    _, repo = _make_pair()
    await repo.enqueue("task", b"data")
    jobs = await repo.dequeue(1, {"task": _ep_params()}, QM_ID, None)
    await repo.log_jobs([(jobs[0], "successful", None)])

    stats = await repo.log_statistics(tail=10)
    assert len(stats) > 0


# -----------------------------------------------------------------------
# job_status
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_job_status() -> None:
    _, repo = _make_pair()
    ids = await repo.enqueue("task", b"data")
    jobs = await repo.dequeue(1, {"task": _ep_params()}, QM_ID, None)
    await repo.log_jobs([(jobs[0], "successful", None)])

    statuses = await repo.job_status(ids)
    assert len(statuses) == 1
    assert statuses[0][1] == "successful"


# -----------------------------------------------------------------------
# Performance
# -----------------------------------------------------------------------


@pytest.mark.anyio
async def test_performance_10k_enqueue_dequeue() -> None:
    """Enqueue + dequeue 10k jobs in under 1 second."""
    import time

    _, repo = _make_pair()

    start = time.monotonic()

    # Batch enqueue.
    n = 10_000
    ids = await repo.enqueue(
        ["task"] * n,
        [b"payload"] * n,
        [0] * n,
    )
    assert len(ids) == n

    # Dequeue all.
    jobs = await repo.dequeue(n, {"task": _ep_params()}, QM_ID, None)
    assert len(jobs) == n

    elapsed = time.monotonic() - start
    assert elapsed < 1.0, f"Performance target missed: {elapsed:.2f}s"
