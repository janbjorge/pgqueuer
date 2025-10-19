"""
Example demonstrating failed job re-queueing functionality.

This example shows how to:
1. Configure entrypoints to mark failed jobs for manual intervention
2. Use CLI commands to manage failed jobs
3. Re-queue failed jobs for reprocessing
"""

import asyncio
import random
from datetime import datetime

import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job


async def main() -> PgQueuer:
    """Create a consumer with entrypoints that handle failures differently."""
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    pgq = PgQueuer(driver)

    @pgq.entrypoint("data_processing", mark_as_failed=True)
    async def process_data(job: Job) -> None:
        """
        Process data with failure handling.
        
        When this job fails, it will be marked as 'failed' instead of being
        moved to the log table, allowing for manual re-queueing.
        """
        print(f"Processing job {job.id} with payload: {job.payload}")
        
        # Simulate random failures for demonstration
        if random.random() < 0.3:  # 30% chance of failure
            raise ValueError(f"Processing failed for job {job.id}")
        
        print(f"Job {job.id} completed successfully at {datetime.now()}")

    @pgq.entrypoint("critical_task", mark_as_failed=True, retry_timer=asyncio.timedelta(minutes=5))
    async def critical_task(job: Job) -> None:
        """
        Critical task that should be manually reviewed when it fails.
        
        Failed jobs will be held for manual intervention rather than
        being automatically retried or discarded.
        """
        print(f"Executing critical task {job.id}")
        
        # Simulate a failure that requires manual intervention
        if job.payload and b"error" in job.payload:
            raise RuntimeError(f"Critical error in job {job.id} - requires manual review")
        
        print(f"Critical task {job.id} completed successfully")

    @pgq.entrypoint("standard_processing")  # Default behavior - no mark_as_failed
    async def standard_processing(job: Job) -> None:
        """
        Standard processing with default failure handling.
        
        When this job fails, it will be moved to the log table as usual.
        """
        print(f"Standard processing of job {job.id}")
        
        if random.random() < 0.2:  # 20% chance of failure
            raise Exception(f"Standard processing failed for job {job.id}")
        
        print(f"Standard job {job.id} completed")

    return pgq


if __name__ == "__main__":
    print("Failed Jobs Example")
    print("==================")
    print()
    print("This example demonstrates the failed job re-queueing functionality.")
    print("Run this consumer and then use the CLI commands to manage failed jobs:")
    print()
    print("1. Start the consumer:")
    print("   pgq run examples.failed_jobs_example:main")
    print()
    print("2. In another terminal, enqueue some jobs:")
    print("   python -c \"")
    print("   import asyncio")
    print("   import asyncpg")
    print("   from pgqueuer.db import AsyncpgDriver")
    print("   from pgqueuer.queries import Queries")
    print("   ")
    print("   async def enqueue_jobs():")
    print("       conn = await asyncpg.connect()")
    print("       driver = AsyncpgDriver(conn)")
    print("       q = Queries(driver)")
    print("       ")
    print("       # Enqueue jobs that may fail and be marked for manual intervention")
    print("       await q.enqueue(['data_processing'] * 10, [b'data'] * 10, [0] * 10)")
    print("       await q.enqueue(['critical_task'], [b'error'], [0])")
    print("       await q.enqueue(['standard_processing'] * 5, [b'std'] * 5, [0] * 5)")
    print("       print('Jobs enqueued!')")
    print("   ")
    print("   asyncio.run(enqueue_jobs())")
    print("   \"")
    print()
    print("3. List failed jobs:")
    print("   pgq list-failed")
    print()
    print("4. Re-queue specific failed jobs:")
    print("   pgq requeue-failed --job-ids \"1,2,3\"")
    print()
    print("5. Manually mark jobs as failed:")
    print("   pgq mark-failed --job-ids \"4,5\"")