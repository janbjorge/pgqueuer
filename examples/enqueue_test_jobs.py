"""
Script to enqueue test jobs for demonstrating failed job functionality.

Usage: python examples/enqueue_test_jobs.py
"""

import asyncio
import asyncpg
from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries


async def main() -> None:
    """Enqueue various types of jobs for testing failed job functionality."""
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)
    q = Queries(driver)

    print("Enqueuing test jobs...")

    # Enqueue jobs for data_processing (some will fail and be marked as failed)
    data_jobs = await q.enqueue(
        entrypoint=["data_processing"] * 8,
        payload=[f"data_{i}".encode() for i in range(8)],
        priority=[0] * 8,
    )
    print(f"Enqueued {len(data_jobs)} data processing jobs: {data_jobs}")

    # Enqueue a critical task that will fail (contains 'error' in payload)
    critical_jobs = await q.enqueue(
        entrypoint=["critical_task"],
        payload=[b"error_data"],
        priority=[1],  # Higher priority
    )
    print(f"Enqueued {len(critical_jobs)} critical task (will fail): {critical_jobs}")

    # Enqueue some standard processing jobs (will use default failure handling)
    standard_jobs = await q.enqueue(
        entrypoint=["standard_processing"] * 5,
        payload=[f"std_{i}".encode() for i in range(5)],
        priority=[0] * 5,
    )
    print(f"Enqueued {len(standard_jobs)} standard processing jobs: {standard_jobs}")

    print(f"\nTotal jobs enqueued: {len(data_jobs) + len(critical_jobs) + len(standard_jobs)}")
    print("\nTo see the jobs in action:")
    print("1. Start the consumer: pgq run examples.failed_jobs_example:main")
    print("2. Watch for failed jobs: pgq list-failed")
    print("3. Re-queue failed jobs: pgq requeue-failed --job-ids \"<job_ids>\"")

    await conn.close()


if __name__ == "__main__":
    asyncio.run(main())