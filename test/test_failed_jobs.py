"""Tests for failed job re-queueing functionality."""
import asyncio
from datetime import timedelta

import pytest

from pgqueuer import db, queries
from pgqueuer.models import Job, JobId
from pgqueuer.qm import QueueManager
from pgqueuer.types import JOB_STATUS


async def test_mark_jobs_as_failed(apgdriver: db.Driver) -> None:
    """Test marking jobs as failed instead of moving to log table."""
    qm = QueueManager(apgdriver)
    
    # Register an entrypoint that always fails with mark_as_failed=True
    @qm.entrypoint("test_fail", mark_as_failed=True)
    async def failing_job(job: Job) -> None:
        raise ValueError("This job always fails")
    
    # Enqueue a job
    job_ids = await qm.queries.enqueue(["test_fail"], [b"test"], [0])
    assert len(job_ids) == 1
    job_id = job_ids[0]
    
    # Process the job (it will fail and should be marked as failed)
    async def run_once() -> None:
        await qm.run(dequeue_timeout=timedelta(seconds=1), batch_size=1)

    # Set a timeout and run
    try:
        async with asyncio.timeout(3):
            task = asyncio.create_task(run_once())
            # Give some time for processing
            await asyncio.sleep(0.5)
            qm.shutdown.set()
            await task
    except asyncio.TimeoutError:
        qm.shutdown.set()

    # Check that the job is marked as failed (not moved to log table)
    failed_jobs = await qm.queries.list_failed_jobs()
    assert len(failed_jobs) == 1
    assert failed_jobs[0].id == job_id
    assert failed_jobs[0].status == "failed"


async def test_requeue_failed_jobs(apgdriver: db.Driver) -> None:
    """Test re-queueing failed jobs back to queued status."""
    q = queries.Queries(apgdriver)
    
    # First enqueue and manually mark a job as failed
    job_ids = await q.enqueue(["test_requeue"], [b"test"], [0])
    job_id = job_ids[0]
    
    # Mark the job as failed
    await q.mark_jobs_as_failed([job_id])
    
    # Verify it's failed
    failed_jobs = await q.list_failed_jobs()
    assert len(failed_jobs) == 1
    assert failed_jobs[0].status == "failed"
    
    # Re-queue the failed job
    requeued_ids = await q.requeue_failed_jobs([job_id])
    assert len(requeued_ids) == 1
    assert requeued_ids[0] == job_id
    
    # Verify it's no longer in failed status
    failed_jobs_after = await q.list_failed_jobs()
    assert len(failed_jobs_after) == 0
    
    # Verify it's back in the queue with 'queued' status
    queue_stats = await q.queue_size()
    queued_stats = [stat for stat in queue_stats if stat.status == "queued"]
    assert len(queued_stats) > 0


async def test_failed_jobs_vs_exception_jobs(apgdriver: db.Driver) -> None:
    """Test difference between mark_as_failed=True and default behavior."""
    qm = QueueManager(apgdriver)
    
    # Register two entrypoints: one with mark_as_failed, one without
    @qm.entrypoint("test_fail_mark", mark_as_failed=True)
    async def failing_job_mark(job: Job) -> None:
        raise ValueError("This job will be marked as failed")
    
    @qm.entrypoint("test_fail_log")  # Default behavior
    async def failing_job_log(job: Job) -> None:
        raise ValueError("This job will be logged as exception")
    
    # Enqueue jobs for both entrypoints
    mark_job_ids = await qm.queries.enqueue(["test_fail_mark"], [b"test1"], [0])
    log_job_ids = await qm.queries.enqueue(["test_fail_log"], [b"test2"], [0])
    
    # Process the jobs
    async def run_jobs() -> None:
        await qm.run(dequeue_timeout=timedelta(seconds=1), batch_size=2)

    try:
        async with asyncio.timeout(3):
            task = asyncio.create_task(run_jobs())
            await asyncio.sleep(0.5)
            qm.shutdown.set()
            await task
    except asyncio.TimeoutError:
        qm.shutdown.set()

    # Check results
    failed_jobs = await qm.queries.list_failed_jobs()
    log_stats = await qm.queries.log_statistics(tail=100)
    
    # The mark_as_failed job should be in failed_jobs
    assert len(failed_jobs) == 1
    assert failed_jobs[0].id == mark_job_ids[0]
    assert failed_jobs[0].status == "failed"
    
    # The regular failing job should be in the log as exception
    exception_logs = [log for log in log_stats if log.status == "exception"]
    assert len(exception_logs) >= 1


async def test_requeue_nonexistent_jobs(apgdriver: db.Driver) -> None:
    """Test re-queueing jobs that don't exist or aren't failed."""
    q = queries.Queries(apgdriver)
    
    # Try to re-queue non-existent job
    fake_job_id = JobId(999999)
    requeued_ids = await q.requeue_failed_jobs([fake_job_id])
    assert len(requeued_ids) == 0
    
    # Enqueue a normal job and try to re-queue it (should fail since it's not failed)
    job_ids = await q.enqueue(["test_normal"], [b"test"], [0])
    requeued_ids = await q.requeue_failed_jobs(job_ids)
    assert len(requeued_ids) == 0  # Should not re-queue non-failed jobs


async def test_list_failed_jobs_empty(apgdriver: db.Driver) -> None:
    """Test listing failed jobs when there are none."""
    q = queries.Queries(apgdriver)
    
    failed_jobs = await q.list_failed_jobs()
    assert len(failed_jobs) == 0