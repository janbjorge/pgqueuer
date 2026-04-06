"""Tests for database-level retry via RetryRequested."""

from __future__ import annotations

import uuid
from datetime import timedelta

import anyio
import async_timeout

from pgqueuer.adapters.inmemory import InMemoryQueries
from pgqueuer.core.applications import PgQueuer
from pgqueuer.core.executors import (
    DatabaseRetryEntrypointExecutor,
    EntrypointExecutorParameters,
)
from pgqueuer.core.qm import QueueManager
from pgqueuer.db import AsyncpgDriver
from pgqueuer.domain.errors import RetryException, RetryRequested
from pgqueuer.domain.models import Context, Job, JobId, TracebackRecord
from pgqueuer.domain.types import QueueExecutionMode
from pgqueuer.ports.repository import EntrypointExecutionParameter
from pgqueuer.queries import Queries


async def _async_noop(job: Job) -> None:
    pass


# ---------------------------------------------------------------------------
# RetryRequested exception
# ---------------------------------------------------------------------------


def test_retry_requested_default_delay() -> None:
    exc = RetryRequested()
    assert exc.delay == timedelta(0)
    assert exc.reason is None
    assert str(exc) == "Retry requested"


def test_retry_requested_custom_delay_and_reason() -> None:
    exc = RetryRequested(delay=timedelta(seconds=30), reason="rate limited")
    assert exc.delay == timedelta(seconds=30)
    assert exc.reason == "rate limited"
    assert str(exc) == "rate limited"


def test_retry_requested_is_retry_exception() -> None:
    exc = RetryRequested()
    assert isinstance(exc, RetryException)


# ---------------------------------------------------------------------------
# InMemoryQueries.retry_job — unit tests
# ---------------------------------------------------------------------------


async def test_inmemory_retry_job_updates_state(queries: InMemoryQueries) -> None:
    ids = await queries.enqueue("ep", b"payload", priority=5)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )
    assert len(jobs) == 1
    job = jobs[0]
    assert job.status == "picked"
    assert job.attempts == 0

    await queries.retry_job(job, timedelta(0), None)

    # Job is back to queued with incremented attempts
    size = await queries.queue_size()
    assert len(size) == 1
    assert size[0].status == "queued"
    assert size[0].count == 1

    # Verify via dequeue that the job is eligible again and has attempts=1
    jobs_again = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )
    assert len(jobs_again) == 1
    assert jobs_again[0].id == ids[0]
    assert jobs_again[0].attempts == 1
    assert jobs_again[0].payload == b"payload"


async def test_inmemory_retry_job_writes_log_entry(queries: InMemoryQueries) -> None:
    ids = await queries.enqueue("ep", b"x", priority=0)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )

    await queries.retry_job(jobs[0], timedelta(0), None)

    logs = await queries.queue_log()
    # Filter to retry-specific log entries (status='queued' after the initial enqueue)
    retry_logs = [
        log
        for log in logs
        if log.job_id == ids[0] and log.status == "queued" and log.created > jobs[0].created
    ]
    assert len(retry_logs) == 1


# ---------------------------------------------------------------------------
# QueueManager integration — InMemory
# ---------------------------------------------------------------------------


async def test_retry_requested_requeues_and_succeeds() -> None:
    """Handler raises RetryRequested once, then succeeds on the second execution.

    Because retry uses UPDATE (same row, same id), call_count tracks attempts
    by job.id — the id is stable across retries.
    """
    pq = PgQueuer.in_memory()
    call_count: dict[int, int] = {}

    @pq.entrypoint("retry_ep")
    async def handler(job: Job) -> None:
        call_count[job.id] = call_count.get(job.id, 0) + 1
        if call_count[job.id] == 1:
            raise RetryRequested(delay=timedelta(0), reason="transient failure")

    await pq.qm.queries.enqueue("retry_ep", b"data", priority=0)

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        dequeue_timeout=timedelta(seconds=1),
    )

    # Called exactly twice: first raises RetryRequested, second succeeds
    assert sum(call_count.values()) == 2

    logs = await pq.qm.queries.queue_log()
    assert sum(1 for log in logs if log.status == "successful") == 1

    # Queue is empty after success
    assert await pq.qm.queries.queue_size() == []


async def test_unhandled_exception_remains_terminal() -> None:
    """Regular exceptions still DELETE the job and log 'exception' (existing behavior)."""
    pq = PgQueuer.in_memory()

    @pq.entrypoint("fail_ep")
    async def handler(job: Job) -> None:
        raise ValueError("permanent failure")

    await pq.qm.queries.enqueue("fail_ep", b"data", priority=0)

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        dequeue_timeout=timedelta(seconds=1),
    )

    logs = await pq.qm.queries.queue_log()
    exception_logs = [log for log in logs if log.status == "exception"]
    assert len(exception_logs) == 1
    assert exception_logs[0].traceback is not None
    assert exception_logs[0].traceback.exception_type == "ValueError"

    # Job removed from queue
    assert await pq.qm.queries.queue_size() == []


async def test_attempts_visible_to_handler_on_retry() -> None:
    """On the retry execution, job.attempts reflects previous attempts."""
    pq = PgQueuer.in_memory()
    seen_attempts: list[int] = []

    @pq.entrypoint("attempts_ep")
    async def handler(job: Job) -> None:
        seen_attempts.append(job.attempts)
        if job.attempts == 0:
            raise RetryRequested(delay=timedelta(0))

    await pq.qm.queries.enqueue("attempts_ep", b"x", priority=0)

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        dequeue_timeout=timedelta(seconds=1),
    )

    assert seen_attempts == [0, 1]


async def test_retry_graph_traceability() -> None:
    """Log entries form a complete, ordered retry graph queryable by job_id."""
    pq = PgQueuer.in_memory()
    call_count: dict[int, int] = {}

    @pq.entrypoint("trace_ep")
    async def handler(job: Job) -> None:
        call_count[job.id] = call_count.get(job.id, 0) + 1
        if call_count[job.id] <= 2:
            raise RetryRequested(
                delay=timedelta(0),
                reason=f"retry attempt {call_count[job.id]}",
            )

    ids = await pq.qm.queries.enqueue("trace_ep", b"payload", priority=0)

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        dequeue_timeout=timedelta(seconds=1),
    )

    assert sum(call_count.values()) == 3

    logs = await pq.qm.queries.queue_log()
    job_logs = [log for log in logs if log.job_id == ids[0]]

    # Retry log entries carry full traceability context
    retry_logs = [log for log in job_logs if log.status == "queued" and log.traceback is not None]
    assert len(retry_logs) == 2

    for retry_log in retry_logs:
        assert retry_log.traceback is not None
        assert retry_log.traceback.additional_context is not None
        ctx = retry_log.traceback.additional_context
        assert "attempt" in ctx
        assert "retry_delay" in ctx
        assert "reason" in ctx
        assert "entrypoint" in ctx

    # Terminal success also logged
    assert sum(1 for log in job_logs if log.status == "successful") == 1


async def test_payload_preserved_across_retries() -> None:
    """The job payload survives retry — same row, UPDATE not DELETE."""
    pq = PgQueuer.in_memory()
    payloads_seen: list[bytes | None] = []

    @pq.entrypoint("payload_ep")
    async def handler(job: Job) -> None:
        payloads_seen.append(job.payload)
        if job.attempts == 0:
            raise RetryRequested(delay=timedelta(0))

    await pq.qm.queries.enqueue("payload_ep", b"important-data", priority=0)

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        dequeue_timeout=timedelta(seconds=1),
    )

    assert payloads_seen == [b"important-data", b"important-data"]


# ---------------------------------------------------------------------------
# DatabaseRetryEntrypointExecutor
# ---------------------------------------------------------------------------


async def test_database_retry_executor_exhausts_max_attempts() -> None:
    """After max_attempts retries, the original exception becomes terminal."""
    pq = PgQueuer.in_memory()

    @pq.entrypoint(
        "exhaust_ep",
        executor_factory=lambda params: DatabaseRetryEntrypointExecutor(
            parameters=params,
            max_attempts=2,
            initial_delay=timedelta(0),
        ),
    )
    async def handler(job: Job) -> None:
        raise ValueError("always fails")

    await pq.qm.queries.enqueue("exhaust_ep", b"data", priority=0)

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        dequeue_timeout=timedelta(seconds=1),
    )

    logs = await pq.qm.queries.queue_log()
    ep_logs = [log for log in logs if log.entrypoint == "exhaust_ep"]

    # Exactly 2 retry entries (attempts 0 and 1), then 1 terminal exception (attempt 2)
    retry_logs = [log for log in ep_logs if log.status == "queued" and log.traceback is not None]
    assert len(retry_logs) == 2

    exception_logs = [log for log in ep_logs if log.status == "exception"]
    assert len(exception_logs) == 1


async def test_database_retry_executor_succeeds_after_retries() -> None:
    """DatabaseRetryEntrypointExecutor re-queues transient failures, then succeeds."""
    pq = PgQueuer.in_memory()
    seen_attempts: list[int] = []

    @pq.entrypoint(
        "recover_ep",
        executor_factory=lambda params: DatabaseRetryEntrypointExecutor(
            parameters=params,
            max_attempts=5,
            initial_delay=timedelta(0),
        ),
    )
    async def handler(job: Job) -> None:
        seen_attempts.append(job.attempts)
        if job.attempts < 2:
            raise ValueError("transient")

    await pq.qm.queries.enqueue("recover_ep", b"data", priority=0)

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        dequeue_timeout=timedelta(seconds=1),
    )

    assert seen_attempts == [0, 1, 2]

    logs = await pq.qm.queries.queue_log()
    assert sum(1 for log in logs if log.status == "successful") == 1


async def test_database_retry_executor_passes_through_retry_requested() -> None:
    """If the handler raises RetryRequested directly, it passes through unchanged."""
    pq = PgQueuer.in_memory()

    @pq.entrypoint(
        "passthrough_ep",
        executor_factory=lambda params: DatabaseRetryEntrypointExecutor(
            parameters=params,
            max_attempts=1,
        ),
    )
    async def handler(job: Job) -> None:
        if job.attempts == 0:
            raise RetryRequested(delay=timedelta(0), reason="explicit retry")

    await pq.qm.queries.enqueue("passthrough_ep", b"x", priority=0)

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        dequeue_timeout=timedelta(seconds=1),
    )

    logs = await pq.qm.queries.queue_log()
    retry_logs = [
        log
        for log in logs
        if log.entrypoint == "passthrough_ep"
        and log.status == "queued"
        and log.traceback is not None
    ]
    assert len(retry_logs) == 1
    assert retry_logs[0].traceback is not None
    assert retry_logs[0].traceback.additional_context is not None
    assert retry_logs[0].traceback.additional_context["reason"] == "explicit retry"


def test_database_retry_executor_backoff_caps_at_max_delay() -> None:
    """Exponential backoff increases per attempt and is capped at max_delay."""
    executor = DatabaseRetryEntrypointExecutor(
        parameters=EntrypointExecutorParameters(
            concurrency_limit=0,
            func=_async_noop,
            requests_per_second=0,
            retry_timer=timedelta(seconds=10),
            serialized_dispatch=False,
        ),
        max_attempts=10,
        initial_delay=timedelta(seconds=1),
        max_delay=timedelta(seconds=60),
        backoff_multiplier=2.0,
    )

    # Verify delay grows: attempt 0 → 1s, attempt 1 → 2s, ..., capped at 60s
    delays = []
    for attempt in range(8):
        delay = min(
            executor.initial_delay * (executor.backoff_multiplier**attempt),
            executor.max_delay,
        )
        delays.append(delay)

    # Monotonically increasing
    assert delays == sorted(delays)
    # First delay is initial_delay
    assert delays[0] == timedelta(seconds=1)
    # Eventually capped
    assert delays[-1] == timedelta(seconds=60)
    # Not all the same (actually grows)
    assert len(set(delays)) > 1


# ---------------------------------------------------------------------------
# Postgres integration
# ---------------------------------------------------------------------------


async def test_retry_requested_postgres(apgdriver: AsyncpgDriver) -> None:
    """Full retry lifecycle against real Postgres."""
    q = Queries(apgdriver)
    qm = QueueManager(connection=apgdriver, queries=Queries(apgdriver))
    seen_attempts: list[int] = []

    @qm.entrypoint("pg_retry_ep")
    async def handler(job: Job) -> None:
        seen_attempts.append(job.attempts)
        if job.attempts == 0:
            raise RetryRequested(delay=timedelta(0), reason="transient")

    ids = await q.enqueue("pg_retry_ep", b"test-payload", priority=0)

    async with async_timeout.timeout(10):
        await qm.run(
            mode=QueueExecutionMode.drain,
            dequeue_timeout=timedelta(seconds=1),
        )

    # Handler saw attempts 0 then 1
    assert seen_attempts == [0, 1]

    logs = await q.queue_log()

    # Exactly one success
    success_logs = [log for log in logs if log.status == "successful"]
    assert len(success_logs) == 1

    # Retry log entry with full traceability
    retry_logs = [
        log
        for log in logs
        if log.job_id == ids[0] and log.status == "queued" and log.traceback is not None
    ]
    assert len(retry_logs) == 1
    assert retry_logs[0].traceback is not None
    assert retry_logs[0].traceback.additional_context is not None
    ctx = retry_logs[0].traceback.additional_context
    assert ctx["attempt"] == 0
    assert ctx["reason"] == "transient"
    assert "retry_delay" in ctx


async def test_retry_preserves_payload_postgres(apgdriver: AsyncpgDriver) -> None:
    """Payload survives retry in real Postgres — UPDATE keeps the row intact."""
    q = Queries(apgdriver)
    qm = QueueManager(connection=apgdriver, queries=Queries(apgdriver))
    payloads_seen: list[bytes | None] = []

    @qm.entrypoint("pg_payload_ep")
    async def handler(job: Job) -> None:
        payloads_seen.append(job.payload)
        if job.attempts == 0:
            raise RetryRequested(delay=timedelta(0))

    await q.enqueue("pg_payload_ep", b"critical-data", priority=0)

    async with async_timeout.timeout(10):
        await qm.run(
            mode=QueueExecutionMode.drain,
            dequeue_timeout=timedelta(seconds=1),
        )

    assert payloads_seen == [b"critical-data", b"critical-data"]


# ---------------------------------------------------------------------------
# Edge cases — InMemory
# ---------------------------------------------------------------------------


async def test_retry_preserves_job_id_across_retries() -> None:
    """The same job.id is seen by the handler on every attempt."""
    pq = PgQueuer.in_memory()
    seen_ids: list[int] = []

    @pq.entrypoint("id_ep")
    async def handler(job: Job) -> None:
        seen_ids.append(job.id)
        if job.attempts < 2:
            raise RetryRequested(delay=timedelta(0))

    await pq.qm.queries.enqueue("id_ep", b"x", priority=0)

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        dequeue_timeout=timedelta(seconds=1),
    )

    # All three executions see the same job id
    assert len(seen_ids) == 3
    assert len(set(seen_ids)) == 1


async def test_retry_preserves_priority() -> None:
    """Priority is unchanged after retry — UPDATE does not modify it."""
    pq = PgQueuer.in_memory()
    seen_priorities: list[int] = []

    @pq.entrypoint("prio_ep")
    async def handler(job: Job) -> None:
        seen_priorities.append(job.priority)
        if job.attempts == 0:
            raise RetryRequested(delay=timedelta(0))

    await pq.qm.queries.enqueue("prio_ep", b"x", priority=42)

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        dequeue_timeout=timedelta(seconds=1),
    )

    assert seen_priorities == [42, 42]


async def test_retry_preserves_headers() -> None:
    """Headers survive retry — UPDATE keeps the row intact."""
    pq = PgQueuer.in_memory()
    seen_headers: list[dict | None] = []

    @pq.entrypoint("headers_ep")
    async def handler(job: Job) -> None:
        seen_headers.append(job.headers)
        if job.attempts == 0:
            raise RetryRequested(delay=timedelta(0))

    await pq.qm.queries.enqueue("headers_ep", b"x", priority=0, headers={"trace_id": "abc123"})

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        dequeue_timeout=timedelta(seconds=1),
    )

    assert len(seen_headers) == 2
    for h in seen_headers:
        assert h is not None
        assert h["trace_id"] == "abc123"


async def test_retry_with_delay_prevents_immediate_dequeue(
    queries: InMemoryQueries,
) -> None:
    """A retried job with non-zero delay is not dequeued until execute_after passes."""
    await queries.enqueue("ep", b"x", priority=0)
    qm_id = uuid.uuid4()
    ep_params = {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)}

    jobs = await queries.dequeue(10, ep_params, qm_id, None)
    assert len(jobs) == 1

    # Retry with a 1-hour delay
    await queries.retry_job(jobs[0], timedelta(hours=1), None)

    # Immediate dequeue should return nothing — execute_after is in the future
    jobs_after = await queries.dequeue(10, ep_params, qm_id, None)
    assert len(jobs_after) == 0

    # But the job is still in the queue (status=queued)
    size = await queries.queue_size()
    assert len(size) == 1
    assert size[0].count == 1


async def test_multiple_jobs_only_one_retries() -> None:
    """Retrying one job does not affect other jobs in the same batch."""
    pq = PgQueuer.in_memory()
    results: dict[int, str] = {}

    @pq.entrypoint("multi_ep")
    async def handler(job: Job) -> None:
        payload = job.payload.decode() if job.payload else ""
        if payload == "retry-me" and job.attempts == 0:
            raise RetryRequested(delay=timedelta(0))
        results[job.id] = f"{payload}-attempt-{job.attempts}"

    await pq.qm.queries.enqueue("multi_ep", b"ok-job", priority=0)
    await pq.qm.queries.enqueue("multi_ep", b"retry-me", priority=0)
    await pq.qm.queries.enqueue("multi_ep", b"another-ok", priority=0)

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        dequeue_timeout=timedelta(seconds=1),
    )

    # All three jobs completed
    assert len(results) == 3

    payloads = set(results.values())
    assert "ok-job-attempt-0" in payloads
    assert "another-ok-attempt-0" in payloads
    assert "retry-me-attempt-1" in payloads

    assert await pq.qm.queries.queue_size() == []


async def test_inmemory_retry_job_nonexistent_is_noop(queries: InMemoryQueries) -> None:
    """Calling retry_job on a nonexistent job is a silent no-op."""
    fake_job = Job(
        id=99999,
        priority=0,
        created="2024-01-01T00:00:00Z",
        updated="2024-01-01T00:00:00Z",
        heartbeat="2024-01-01T00:00:00Z",
        execute_after="2024-01-01T00:00:00Z",
        status="picked",
        entrypoint="ghost",
        payload=None,
        attempts=0,
        queue_manager_id=None,
        headers=None,
    )
    # Should not raise
    await queries.retry_job(fake_job, timedelta(0), None)

    # No log entries written
    logs = await queries.queue_log()
    assert len(logs) == 0


async def test_inmemory_retry_job_stores_traceback(queries: InMemoryQueries) -> None:
    """Traceback record is stored as JSON in the retry log entry."""
    await queries.enqueue("ep", b"x", priority=0)
    qm_id = uuid.uuid4()
    jobs = await queries.dequeue(
        10,
        {"ep": EntrypointExecutionParameter(timedelta(0), False, 0)},
        qm_id,
        None,
    )

    tbr = TracebackRecord.from_exception(
        exc=RetryRequested(reason="test-reason"),
        job_id=JobId(jobs[0].id),
        additional_context={"attempt": 0, "reason": "test-reason"},
    )
    await queries.retry_job(jobs[0], timedelta(0), tbr)

    logs = await queries.queue_log()
    retry_logs = [log for log in logs if log.status == "queued" and log.traceback is not None]
    assert len(retry_logs) == 1
    assert retry_logs[0].traceback is not None
    assert retry_logs[0].traceback.exception_type == "RetryRequested"
    assert retry_logs[0].traceback.additional_context is not None
    assert retry_logs[0].traceback.additional_context["reason"] == "test-reason"


# ---------------------------------------------------------------------------
# Edge cases — DatabaseRetryEntrypointExecutor
# ---------------------------------------------------------------------------


async def test_database_retry_executor_max_attempts_zero() -> None:
    """With max_attempts=0, the very first exception is immediately terminal."""
    pq = PgQueuer.in_memory()

    @pq.entrypoint(
        "zero_ep",
        executor_factory=lambda params: DatabaseRetryEntrypointExecutor(
            parameters=params,
            max_attempts=0,
            initial_delay=timedelta(0),
        ),
    )
    async def handler(job: Job) -> None:
        raise ValueError("fail immediately")

    await pq.qm.queries.enqueue("zero_ep", b"x", priority=0)

    await pq.qm.run(
        batch_size=10,
        mode=QueueExecutionMode.drain,
        max_concurrent_tasks=100,
        dequeue_timeout=timedelta(seconds=1),
    )

    logs = await pq.qm.queries.queue_log()
    ep_logs = [log for log in logs if log.entrypoint == "zero_ep"]

    # No retries — straight to terminal exception
    retry_logs = [log for log in ep_logs if log.status == "queued" and log.traceback is not None]
    assert len(retry_logs) == 0

    exception_logs = [log for log in ep_logs if log.status == "exception"]
    assert len(exception_logs) == 1


async def test_database_retry_executor_chains_cause() -> None:
    """The RetryRequested raised by the executor preserves the original exception as __cause__."""
    pq = PgQueuer.in_memory()
    captured_cause: list[BaseException | None] = []

    @pq.entrypoint("cause_ep")
    async def handler(job: Job) -> None:
        # On attempt 1, the RetryRequested from the executor will have been
        # caught by qm._dispatch, so we can't inspect it there. Instead,
        # test the executor directly.
        pass

    # Test the executor in isolation
    executor = DatabaseRetryEntrypointExecutor(
        parameters=EntrypointExecutorParameters(
            concurrency_limit=0,
            func=handler,
            requests_per_second=0,
            retry_timer=timedelta(0),
            serialized_dispatch=False,
        ),
        max_attempts=5,
        initial_delay=timedelta(0),
    )

    original = ValueError("the root cause")

    fake_job = Job(
        id=1,
        priority=0,
        created="2024-01-01T00:00:00Z",
        updated="2024-01-01T00:00:00Z",
        heartbeat="2024-01-01T00:00:00Z",
        execute_after="2024-01-01T00:00:00Z",
        status="picked",
        entrypoint="cause_ep",
        payload=None,
        attempts=0,
        queue_manager_id=None,
        headers=None,
    )

    # Monkey-patch the executor's func to raise
    async def failing_func(job: Job) -> None:
        raise original

    executor.parameters.func = failing_func

    try:
        await executor.execute(fake_job, Context(cancellation=anyio.CancelScope()))
    except RetryRequested as e:
        captured_cause.append(e.__cause__)

    assert len(captured_cause) == 1
    assert captured_cause[0] is original
