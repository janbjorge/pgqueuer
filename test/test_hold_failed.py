"""Tests for holding failed jobs via on_failure='hold'."""

from __future__ import annotations

import uuid
from datetime import timedelta

import async_timeout
import pytest

from pgqueuer.adapters.inmemory import InMemoryQueries
from pgqueuer.core.applications import PgQueuer
from pgqueuer.core.executors import DatabaseRetryEntrypointExecutor
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.models import Job
from pgqueuer.domain.types import QueueExecutionMode
from pgqueuer.ports.repository import EntrypointExecutionParameter
from pgqueuer.queries import Queries

EP = EntrypointExecutionParameter(0)


# ---------------------------------------------------------------------------
# InMemory: log_jobs partitions correctly
# ---------------------------------------------------------------------------


async def test_inmemory_hold_keeps_job(queries: InMemoryQueries) -> None:
    """on_failure='hold' keeps the job in the queue with status='failed'."""
    await queries.enqueue("ep", b"important-payload", priority=1)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    assert len(jobs) == 1
    job = jobs[0]

    # Simulate buffer flush with "failed" status
    await queries.log_jobs([(job, "failed", None)])

    # Job is still in queue with status='failed'
    failed = await queries.list_failed_jobs()
    assert len(failed) == 1
    assert failed[0].id == job.id
    assert failed[0].payload == b"important-payload"
    assert failed[0].status == "failed"


async def test_inmemory_delete_removes_job(queries: InMemoryQueries) -> None:
    """Default on_failure='delete' removes the job from the queue."""
    await queries.enqueue("ep", b"payload", priority=1)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    job = jobs[0]

    await queries.log_jobs([(job, "exception", None)])

    failed = await queries.list_failed_jobs()
    assert len(failed) == 0


async def test_inmemory_mixed_batch(queries: InMemoryQueries) -> None:
    """Buffer flush handles a mix of failed and exception jobs."""
    await queries.enqueue(
        ["ep", "ep"],
        [b"hold-me", b"delete-me"],
        [1, 1],
    )
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    assert len(jobs) == 2

    await queries.log_jobs(
        [
            (jobs[0], "failed", None),
            (jobs[1], "exception", None),
        ]
    )

    failed = await queries.list_failed_jobs()
    assert len(failed) == 1
    assert failed[0].payload == b"hold-me"


async def test_inmemory_failed_not_dequeued(queries: InMemoryQueries) -> None:
    """Failed jobs are not returned by dequeue."""
    await queries.enqueue("ep", b"payload", priority=1)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    await queries.log_jobs([(jobs[0], "failed", None)])

    jobs2 = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    assert len(jobs2) == 0


async def test_inmemory_requeue_moves_to_queued(queries: InMemoryQueries) -> None:
    """requeue_jobs moves failed jobs back to queued."""
    await queries.enqueue("ep", b"payload", priority=5)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    await queries.log_jobs([(jobs[0], "failed", None)])

    await queries.requeue_jobs([jobs[0].id])

    # Now dequeue should return the job
    jobs2 = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    assert len(jobs2) == 1
    assert jobs2[0].id == jobs[0].id
    assert jobs2[0].attempts == 0
    assert jobs2[0].status == "picked"


async def test_inmemory_requeue_resets_attempts(queries: InMemoryQueries) -> None:
    """Re-queued jobs have attempts reset to 0."""
    await queries.enqueue("ep", b"payload", priority=1)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    job = jobs[0]

    # Simulate a few retries then hold
    await queries.retry_job(job, timedelta(0), None)
    await queries.retry_job(job, timedelta(0), None)
    jobs2 = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    assert jobs2[0].attempts == 2

    await queries.log_jobs([(jobs2[0], "failed", None)])
    await queries.requeue_jobs([jobs2[0].id])

    jobs3 = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    assert jobs3[0].attempts == 0


async def test_inmemory_requeue_ignores_non_failed(queries: InMemoryQueries) -> None:
    """requeue_jobs only affects jobs with status='failed'."""
    await queries.enqueue("ep", b"payload", priority=1)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    await queries.requeue_jobs([jobs[0].id])
    failed = await queries.list_failed_jobs()
    assert len(failed) == 0


async def test_inmemory_list_failed_ordered_by_created(queries: InMemoryQueries) -> None:
    """list_failed_jobs returns jobs ordered by created DESC."""
    for i in range(3):
        await queries.enqueue("ep", f"job-{i}".encode(), priority=1)

    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    for j in jobs:
        await queries.log_jobs([(j, "failed", None)])

    failed = await queries.list_failed_jobs()
    assert len(failed) == 3
    # Most recently created first
    assert failed[0].created >= failed[1].created >= failed[2].created


async def test_inmemory_list_failed_respects_limit(queries: InMemoryQueries) -> None:
    """list_failed_jobs respects the limit parameter."""
    for i in range(5):
        await queries.enqueue("ep", f"job-{i}".encode(), priority=1)

    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    for j in jobs:
        await queries.log_jobs([(j, "failed", None)])

    failed = await queries.list_failed_jobs(limit=2)
    assert len(failed) == 2


async def test_inmemory_failed_releases_dedupe_key(queries: InMemoryQueries) -> None:
    """A held/failed job must release its dedupe_key so a new job can be enqueued."""
    await queries.enqueue("ep", b"first", priority=1, dedupe_key="unique-key")
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10, {"ep": EP}, qm_id, None, heartbeat_timeout=timedelta(seconds=30)
    )
    assert len(jobs) == 1

    # Hold the job as failed
    await queries.log_jobs([(jobs[0], "failed", None)])

    # A new job with the same dedupe_key should succeed (matches PostgreSQL behavior
    # where the unique index only covers status IN ('queued', 'picked'))
    ids = await queries.enqueue("ep", b"second", priority=1, dedupe_key="unique-key")
    assert len(ids) == 1


# ---------------------------------------------------------------------------
# Integration: PgQueuer with on_failure="hold"
# ---------------------------------------------------------------------------


async def test_hold_keeps_job_in_queue(apgdriver: AsyncpgDriver) -> None:
    """End-to-end: on_failure='hold' parks the job with status='failed'."""
    queries = Queries(apgdriver)
    pgq = PgQueuer(apgdriver)

    processed: list[Job] = []

    @pgq.entrypoint("hold_test", on_failure="hold")
    async def handler(job: Job) -> None:
        processed.append(job)
        raise RuntimeError("boom")

    await queries.enqueue("hold_test", b"precious-payload", priority=1)

    async with async_timeout.timeout(10):
        await pgq.run(dequeue_timeout=timedelta(seconds=0.1), mode=QueueExecutionMode.drain)

    assert len(processed) == 1

    failed = await queries.list_failed_jobs()
    assert len(failed) == 1
    assert failed[0].payload == b"precious-payload"
    assert failed[0].status == "failed"
    assert failed[0].entrypoint == "hold_test"


async def test_delete_default_removes_job(apgdriver: AsyncpgDriver) -> None:
    """Default on_failure='delete' removes the job as before."""
    queries = Queries(apgdriver)
    pgq = PgQueuer(apgdriver)

    @pgq.entrypoint("delete_test")
    async def handler(job: Job) -> None:
        raise RuntimeError("boom")

    await queries.enqueue("delete_test", b"payload", priority=1)

    async with async_timeout.timeout(10):
        await pgq.run(dequeue_timeout=timedelta(seconds=0.1), mode=QueueExecutionMode.drain)

    failed = await queries.list_failed_jobs()
    assert len(failed) == 0


async def test_hold_full_lifecycle(apgdriver: AsyncpgDriver) -> None:
    """Full lifecycle: fail -> hold -> list -> requeue -> process successfully."""
    queries = Queries(apgdriver)
    pgq = PgQueuer(apgdriver)

    call_count = 0

    @pgq.entrypoint("lifecycle_test", on_failure="hold")
    async def handler(job: Job) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("first attempt fails")
        # Second attempt succeeds

    await queries.enqueue("lifecycle_test", b"data", priority=1)

    # First run: job fails and is held
    async with async_timeout.timeout(10):
        await pgq.run(dequeue_timeout=timedelta(seconds=0.1), mode=QueueExecutionMode.drain)

    assert call_count == 1
    failed = await queries.list_failed_jobs()
    assert len(failed) == 1

    # Re-queue the failed job
    await queries.requeue_jobs([failed[0].id])
    failed_after = await queries.list_failed_jobs()
    assert len(failed_after) == 0

    # Second run: job succeeds
    pgq2 = PgQueuer(apgdriver)

    @pgq2.entrypoint("lifecycle_test", on_failure="hold")
    async def handler2(job: Job) -> None:
        nonlocal call_count
        call_count += 1

    async with async_timeout.timeout(10):
        await pgq2.run(dequeue_timeout=timedelta(seconds=0.1), mode=QueueExecutionMode.drain)

    assert call_count == 2
    failed_final = await queries.list_failed_jobs()
    assert len(failed_final) == 0


async def test_hold_writes_log_entry(apgdriver: AsyncpgDriver) -> None:
    """Holding a job writes a log entry with status='failed'."""
    queries = Queries(apgdriver)
    pgq = PgQueuer(apgdriver)

    @pgq.entrypoint("log_test", on_failure="hold")
    async def handler(job: Job) -> None:
        raise ValueError("test error")

    await queries.enqueue("log_test", b"payload", priority=1)

    async with async_timeout.timeout(10):
        await pgq.run(dequeue_timeout=timedelta(seconds=0.1), mode=QueueExecutionMode.drain)

    # Check log table has an entry
    log = await queries.queue_log()
    failed_logs = [entry for entry in log if entry.status == "failed"]
    assert len(failed_logs) >= 1


async def test_requeue_resets_attempts_postgres(apgdriver: AsyncpgDriver) -> None:
    """Re-queued jobs have attempts reset to 0 in Postgres."""
    queries = Queries(apgdriver)
    pgq = PgQueuer(apgdriver)

    @pgq.entrypoint(
        "retry_hold_test",
        on_failure="hold",
        executor_factory=lambda params: DatabaseRetryEntrypointExecutor(
            parameters=params,
            max_attempts=2,
            initial_delay=timedelta(seconds=0),
        ),
    )
    async def handler(job: Job) -> None:
        raise RuntimeError("always fails")

    await queries.enqueue("retry_hold_test", b"payload", priority=1)

    async with async_timeout.timeout(10):
        await pgq.run(dequeue_timeout=timedelta(seconds=0.1), mode=QueueExecutionMode.drain)

    failed = await queries.list_failed_jobs()
    assert len(failed) == 1
    assert failed[0].attempts > 0

    await queries.requeue_jobs([failed[0].id])

    # The job is back in queue; dequeue and check attempts
    pgq2 = PgQueuer(apgdriver)
    attempts_seen: list[int] = []

    @pgq2.entrypoint("retry_hold_test", on_failure="hold")
    async def handler2(job: Job) -> None:
        attempts_seen.append(job.attempts)
        raise RuntimeError("fails again")

    async with async_timeout.timeout(10):
        await pgq2.run(dequeue_timeout=timedelta(seconds=0.1), mode=QueueExecutionMode.drain)

    assert attempts_seen[0] == 0


async def test_database_retry_executor_then_hold(apgdriver: AsyncpgDriver) -> None:
    """DatabaseRetryEntrypointExecutor retries N times, then holds on terminal failure."""
    queries = Queries(apgdriver)
    pgq = PgQueuer(apgdriver)

    call_count = 0

    @pgq.entrypoint(
        "retry_then_hold",
        on_failure="hold",
        executor_factory=lambda params: DatabaseRetryEntrypointExecutor(
            parameters=params,
            max_attempts=2,
            initial_delay=timedelta(seconds=0),
        ),
    )
    async def handler(job: Job) -> None:
        nonlocal call_count
        call_count += 1
        raise RuntimeError("always fails")

    await queries.enqueue("retry_then_hold", b"payload", priority=1)

    async with async_timeout.timeout(10):
        await pgq.run(dequeue_timeout=timedelta(seconds=0.1), mode=QueueExecutionMode.drain)

    # Should have been called 3 times: attempt 0, 1, 2 (max_attempts=2 means retry twice)
    assert call_count == 3

    failed = await queries.list_failed_jobs()
    assert len(failed) == 1
    assert failed[0].entrypoint == "retry_then_hold"
    assert failed[0].payload == b"payload"


# ---------------------------------------------------------------------------
# Validation: on_failure must be a valid literal value
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("bad_value", ["cancel", "retry", "", 0, None, True])
async def test_on_failure_rejects_invalid_values(
    apgdriver: AsyncpgDriver,
    bad_value: object,
) -> None:
    """Entrypoint raises ValueError for invalid on_failure values."""
    pgq = PgQueuer(apgdriver)
    with pytest.raises(ValueError, match="on_failure"):

        @pgq.entrypoint("bad_ep", on_failure=bad_value)  # type: ignore[arg-type]
        async def handler(job: Job) -> None: ...


@pytest.mark.parametrize("good_value", ["delete", "hold"])
async def test_on_failure_accepts_valid_values(
    apgdriver: AsyncpgDriver,
    good_value: object,
) -> None:
    """Entrypoint accepts valid on_failure values without error."""
    pgq = PgQueuer(apgdriver)

    @pgq.entrypoint(f"good_ep_{good_value}", on_failure=good_value)  # type: ignore[arg-type]
    async def handler(job: Job) -> None: ...


# ---------------------------------------------------------------------------
# Integration: failed job releases dedupe_key (PostgreSQL)
# ---------------------------------------------------------------------------


async def test_pg_failed_releases_dedupe_key(apgdriver: AsyncpgDriver) -> None:
    """A held/failed job must release its dedupe_key so a new job can be enqueued."""
    queries = Queries(apgdriver)
    pgq = PgQueuer(apgdriver)

    @pgq.entrypoint("dedupe_hold", on_failure="hold")
    async def handler(job: Job) -> None:
        raise RuntimeError("fail")

    await queries.enqueue("dedupe_hold", b"first", priority=1, dedupe_key="dk1")

    async with async_timeout.timeout(10):
        await pgq.run(dequeue_timeout=timedelta(seconds=0.1), mode=QueueExecutionMode.drain)

    failed = await queries.list_failed_jobs()
    assert len(failed) == 1

    # A new job with the same dedupe_key should succeed because the failed job
    # is excluded from the unique index (status IN ('queued', 'picked')).
    ids = await queries.enqueue("dedupe_hold", b"second", priority=1, dedupe_key="dk1")
    assert len(ids) == 1
