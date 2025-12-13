"""
Tests for the manual retry API functionality.

This module tests the ability to:
1. Retrieve failed jobs from the log table
2. Retry failed jobs by re-enqueueing them
3. Track retry relationships via the retried_as column
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import timedelta

from pgqueuer import db, models, queries
from pgqueuer.qm import QueueManager

# Default timeout for all test operations
TEST_TIMEOUT = timedelta(seconds=10)


async def wait_for_log_condition(
    qm: QueueManager,
    condition: Callable[[list[models.Log]], bool],
    timeout: float = 5.0,
    poll_interval: float = 0.05,
) -> None:
    """Wait until a condition on the log table is met, with timeout."""
    deadline = asyncio.get_event_loop().time() + timeout
    while asyncio.get_event_loop().time() < deadline:
        logs = await qm.queries.queue_log()
        if condition(logs):
            return
        await asyncio.sleep(poll_interval)
    raise TimeoutError(f"Condition not met within {timeout}s")


async def run_until_condition(
    qm: QueueManager,
    condition: Callable[[list[models.Log]], bool],
    timeout: float = 5.0,
) -> None:
    """Run the queue manager until a condition is met, then shutdown."""

    async def monitor() -> None:
        try:
            await wait_for_log_condition(qm, condition, timeout=timeout)
        finally:
            qm.shutdown.set()

    await asyncio.wait_for(
        asyncio.gather(
            qm.run(dequeue_timeout=timedelta(seconds=0)),
            monitor(),
        ),
        timeout=timeout + 2.0,
    )


async def test_failed_job_logged_with_payload_and_headers(apgdriver: db.Driver) -> None:
    """Verify that failed jobs are logged with their payload and headers preserved."""
    qm = QueueManager(apgdriver)
    payload = b"test-payload-data"
    headers = {"custom-header": "header-value", "trace-id": "abc123"}

    @qm.entrypoint("failing_job")
    async def failing_job(job: models.Job) -> None:
        raise ValueError("Intentional failure for testing")

    # Enqueue a job with payload and headers
    job_ids = await qm.queries.enqueue(
        entrypoint="failing_job",
        payload=payload,
        headers=headers,
    )
    assert len(job_ids) == 1
    original_job_id = job_ids[0]

    # Run until the job fails
    await run_until_condition(
        qm,
        lambda logs: any(
            log.job_id == original_job_id and log.status == "exception" for log in logs
        ),
    )

    # Verify the log entry has payload and headers
    logs = await qm.queries.queue_log()
    failed_log = next(
        log for log in logs if log.job_id == original_job_id and log.status == "exception"
    )

    assert failed_log.status == "exception"
    assert failed_log.payload == payload
    assert failed_log.headers == headers
    assert failed_log.retried_as is None


async def test_get_failed_jobs_returns_only_failed(apgdriver: db.Driver) -> None:
    """Verify get_failed_jobs only returns jobs with status='exception'."""
    qm = QueueManager(apgdriver)

    @qm.entrypoint("sometimes_fails")
    async def sometimes_fails(job: models.Job) -> None:
        # First job fails, second succeeds
        if job.payload == b"fail":
            raise ValueError("Intentional failure")

    # Enqueue one failing and one succeeding job
    await qm.queries.enqueue(
        entrypoint=["sometimes_fails", "sometimes_fails"],
        payload=[b"fail", b"succeed"],
        priority=[0, 0],
    )

    # Run until both jobs reach terminal status (successful or exception)
    await run_until_condition(
        qm,
        lambda logs: (
            any(log.status == "exception" for log in logs)
            and any(log.status == "successful" for log in logs)
        ),
    )

    # Get failed jobs
    failed_jobs = await qm.queries.get_failed_jobs()

    assert len(failed_jobs) == 1
    assert failed_jobs[0].status == "exception"
    assert failed_jobs[0].payload == b"fail"


async def test_get_failed_jobs_filter_by_entrypoint(apgdriver: db.Driver) -> None:
    """Verify get_failed_jobs can filter by entrypoint."""
    qm = QueueManager(apgdriver)

    @qm.entrypoint("entrypoint_a")
    async def entrypoint_a(job: models.Job) -> None:
        raise ValueError("Failure A")

    @qm.entrypoint("entrypoint_b")
    async def entrypoint_b(job: models.Job) -> None:
        raise ValueError("Failure B")

    # Enqueue jobs for both entrypoints
    await qm.queries.enqueue(
        entrypoint=["entrypoint_a", "entrypoint_b", "entrypoint_a"],
        payload=[b"a1", b"b1", b"a2"],
        priority=[0, 0, 0],
    )

    # Run until all jobs fail
    await run_until_condition(
        qm,
        lambda logs: len([log for log in logs if log.status == "exception"]) >= 3,
    )

    # Filter by entrypoint_a
    failed_a = await qm.queries.get_failed_jobs(entrypoint="entrypoint_a")
    assert len(failed_a) == 2
    assert all(log.entrypoint == "entrypoint_a" for log in failed_a)

    # Filter by entrypoint_b
    failed_b = await qm.queries.get_failed_jobs(entrypoint="entrypoint_b")
    assert len(failed_b) == 1
    assert failed_b[0].entrypoint == "entrypoint_b"

    # Filter by multiple entrypoints
    failed_both = await qm.queries.get_failed_jobs(entrypoint=["entrypoint_a", "entrypoint_b"])
    assert len(failed_both) == 3


async def test_retry_failed_job_creates_new_job(apgdriver: db.Driver) -> None:
    """Verify retry_failed_job creates a new job with the same payload."""
    qm = QueueManager(apgdriver)
    execution_count = 0
    payload = b"retry-me"

    @qm.entrypoint("retry_test")
    async def retry_test(job: models.Job) -> None:
        nonlocal execution_count
        execution_count += 1
        if execution_count == 1:
            raise ValueError("First attempt fails")
        # Second attempt succeeds

    # Enqueue and let it fail
    await qm.queries.enqueue(entrypoint="retry_test", payload=payload)

    await run_until_condition(
        qm,
        lambda logs: any(log.status == "exception" for log in logs),
    )

    assert execution_count == 1

    # Get the failed job and retry it
    failed_jobs = await qm.queries.get_failed_jobs(entrypoint="retry_test")
    assert len(failed_jobs) == 1

    new_job_id = await qm.queries.retry_failed_job(failed_jobs[0].id)
    assert new_job_id is not None

    # Verify the log entry was updated with retried_as
    log_entry = await qm.queries.get_log_entry(failed_jobs[0].id)
    assert log_entry is not None
    assert log_entry.retried_as == new_job_id

    # Run again to process the retried job
    qm.shutdown.clear()

    await run_until_condition(
        qm,
        lambda logs: any(log.status == "successful" for log in logs),
    )

    assert execution_count == 2


async def test_retry_failed_job_with_priority_override(apgdriver: db.Driver) -> None:
    """Verify retry_failed_job can override the priority."""
    qm = QueueManager(apgdriver)

    @qm.entrypoint("priority_test")
    async def priority_test(job: models.Job) -> None:
        raise ValueError("Always fails")

    # Enqueue with low priority
    await qm.queries.enqueue(entrypoint="priority_test", payload=b"test", priority=1)

    await run_until_condition(
        qm,
        lambda logs: any(log.status == "exception" for log in logs),
    )

    # Retry with higher priority
    failed_jobs = await qm.queries.get_failed_jobs()
    assert len(failed_jobs) == 1
    assert failed_jobs[0].priority == 1

    new_job_id = await qm.queries.retry_failed_job(failed_jobs[0].id, priority=10)
    assert new_job_id is not None

    # Verify the new job has the overridden priority
    queue_size = await qm.queries.queue_size()
    high_priority_jobs = [qs for qs in queue_size if qs.priority == 10]
    assert len(high_priority_jobs) == 1


async def test_retry_failed_job_with_execute_after(apgdriver: db.Driver) -> None:
    """Verify retry_failed_job can set execute_after delay."""
    qm = QueueManager(apgdriver)

    @qm.entrypoint("delay_test")
    async def delay_test(job: models.Job) -> None:
        raise ValueError("Always fails")

    await qm.queries.enqueue(entrypoint="delay_test", payload=b"test")

    await run_until_condition(
        qm,
        lambda logs: any(log.status == "exception" for log in logs),
    )

    # Retry with a delay
    failed_jobs = await qm.queries.get_failed_jobs()
    new_job_id = await qm.queries.retry_failed_job(
        failed_jobs[0].id,
        execute_after=timedelta(hours=1),
    )
    assert new_job_id is not None


async def test_retry_already_retried_job_returns_none(apgdriver: db.Driver) -> None:
    """Verify that retrying an already-retried job returns None."""
    qm = QueueManager(apgdriver)

    @qm.entrypoint("double_retry_test")
    async def double_retry_test(job: models.Job) -> None:
        raise ValueError("Always fails")

    await qm.queries.enqueue(entrypoint="double_retry_test", payload=b"test")

    await run_until_condition(
        qm,
        lambda logs: any(log.status == "exception" for log in logs),
    )

    failed_jobs = await qm.queries.get_failed_jobs()
    log_id = failed_jobs[0].id

    # First retry should succeed
    first_retry = await qm.queries.retry_failed_job(log_id)
    assert first_retry is not None

    # Second retry should return None (already retried)
    second_retry = await qm.queries.retry_failed_job(log_id)
    assert second_retry is None


async def test_retry_failed_jobs_bulk(apgdriver: db.Driver) -> None:
    """Verify retry_failed_jobs can retry multiple jobs at once."""
    qm = QueueManager(apgdriver)

    @qm.entrypoint("bulk_retry_test")
    async def bulk_retry_test(job: models.Job) -> None:
        raise ValueError("Always fails")

    # Enqueue multiple jobs
    await qm.queries.enqueue(
        entrypoint=["bulk_retry_test"] * 3,
        payload=[b"job1", b"job2", b"job3"],
        priority=[0, 0, 0],
    )

    await run_until_condition(
        qm,
        lambda logs: len([log for log in logs if log.status == "exception"]) >= 3,
    )

    # Retry all failed jobs at once
    failed_jobs = await qm.queries.get_failed_jobs()
    assert len(failed_jobs) == 3

    log_ids = [log.id for log in failed_jobs]
    new_job_ids = await qm.queries.retry_failed_jobs(log_ids)
    assert len(new_job_ids) == 3

    # Verify all log entries were updated
    for log_id in log_ids:
        log_entry = await qm.queries.get_log_entry(log_id)
        assert log_entry is not None
        assert log_entry.retried_as is not None


async def test_get_failed_jobs_excludes_retried(apgdriver: db.Driver) -> None:
    """Verify get_failed_jobs excludes jobs that have already been retried."""
    qm = QueueManager(apgdriver)

    @qm.entrypoint("exclude_test")
    async def exclude_test(job: models.Job) -> None:
        raise ValueError("Always fails")

    await qm.queries.enqueue(
        entrypoint=["exclude_test"] * 2,
        payload=[b"job1", b"job2"],
        priority=[0, 0],
    )

    await run_until_condition(
        qm,
        lambda logs: len([log for log in logs if log.status == "exception"]) >= 2,
    )

    # Get all failed jobs
    failed_jobs = await qm.queries.get_failed_jobs()
    assert len(failed_jobs) == 2

    # Retry one of them
    await qm.queries.retry_failed_job(failed_jobs[0].id)

    # Now get_failed_jobs should only return the non-retried one
    remaining_failed = await qm.queries.get_failed_jobs()
    assert len(remaining_failed) == 1
    assert remaining_failed[0].id == failed_jobs[1].id


async def test_get_log_entry_returns_none_for_nonexistent(apgdriver: db.Driver) -> None:
    """Verify get_log_entry returns None for non-existent ID."""
    q = queries.Queries(apgdriver)
    result = await q.get_log_entry(999999)
    assert result is None


async def test_get_failed_jobs_cursor_pagination(apgdriver: db.Driver) -> None:
    """Verify get_failed_jobs supports cursor-based pagination with after_id."""
    qm = QueueManager(apgdriver)

    @qm.entrypoint("pagination_test")
    async def pagination_test(job: models.Job) -> None:
        raise ValueError("Always fails")

    # Enqueue 5 jobs
    await qm.queries.enqueue(
        entrypoint=["pagination_test"] * 5,
        payload=[f"job{i}".encode() for i in range(5)],
        priority=[0] * 5,
    )

    await run_until_condition(
        qm,
        lambda logs: len([log for log in logs if log.status == "exception"]) >= 5,
    )

    # Test cursor-based pagination
    # First page - no cursor
    page1 = await qm.queries.get_failed_jobs(limit=2)
    assert len(page1) == 2

    # Second page - use last id from page1 as cursor
    page2 = await qm.queries.get_failed_jobs(limit=2, after_id=page1[-1].id)
    assert len(page2) == 2

    # Third page - use last id from page2 as cursor
    page3 = await qm.queries.get_failed_jobs(limit=2, after_id=page2[-1].id)
    assert len(page3) == 1

    # Verify no overlap and correct ordering (descending by id)
    all_ids = [log.id for log in page1 + page2 + page3]
    assert len(all_ids) == len(set(all_ids))
    assert all_ids == sorted(all_ids, reverse=True)
