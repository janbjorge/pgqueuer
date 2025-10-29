"""
Example demonstrating Context parameter injection in PgQueuer entrypoints.

Shows how to:
- Inject Context to access shared resources and cancellation scopes
- Use both async and sync entrypoints with Context
- Implement custom executors with Context
"""

from __future__ import annotations

import asyncio
import os

import asyncpg

from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.executors import AbstractEntrypointExecutor
from pgqueuer.logconfig import logger
from pgqueuer.models import Context, Job


async def example_async_with_context() -> None:
    """Demonstrate async entrypoint with Context injection."""
    dsn = os.getenv(
        "DATABASE_URL",
        "postgresql://pgquser:pgqpw@localhost:5432/pgqdb",
    )
    conn = await asyncpg.connect(dsn)
    driver = AsyncpgDriver(conn)

    resources: dict[str, object] = {"logger": logger}
    pgq = PgQueuer(driver, resources=resources)

    @pgq.entrypoint("fetch_data")
    async def fetch_data(job: Job, context: Context) -> None:
        """Async entrypoint receiving Job and Context."""
        logger.info(f"Processing job {job.id} with context")

        # Use cancellation scope for graceful shutdown
        with context.cancellation:
            await asyncio.sleep(0.1)
            logger.info(f"Completed job {job.id}")

    logger.info("✓ Async with Context entrypoint registered")
    await conn.close()


async def example_sync_with_context() -> None:
    """Demonstrate sync entrypoint with Context injection."""
    dsn = os.getenv(
        "DATABASE_URL",
        "postgresql://pgquser:pgqpw@localhost:5432/pgqdb",
    )
    conn = await asyncpg.connect(dsn)
    driver = AsyncpgDriver(conn)

    resources: dict[str, object] = {"logger": logger}
    pgq = PgQueuer(driver, resources=resources)

    @pgq.entrypoint("process_batch")
    def process_batch(job: Job, context: Context) -> None:
        """Sync entrypoint receiving Job and Context (runs in thread)."""
        cache = context.resources.get("cache", {})
        if isinstance(cache, dict):
            cache[str(job.id)] = "processed"
        logger.info(f"Batch processing completed for job {job.id}")

    logger.info("✓ Sync with Context entrypoint registered")
    await conn.close()


async def example_without_context() -> None:
    """Demonstrate entrypoint without Context (optional parameter)."""
    dsn = os.getenv(
        "DATABASE_URL",
        "postgresql://pgquser:pgqpw@localhost:5432/pgqdb",
    )
    conn = await asyncpg.connect(dsn)
    driver = AsyncpgDriver(conn)

    pgq = PgQueuer(driver)

    @pgq.entrypoint("simple_task")
    async def simple_task(job: Job) -> None:
        """This entrypoint only receives the Job parameter."""
        logger.info(f"Simple task processing: {job.id}")
        await asyncio.sleep(0.05)

    logger.info("✓ Without Context entrypoint registered")
    await conn.close()


class LoggingExecutor(AbstractEntrypointExecutor):
    """Custom executor that logs job execution with context."""

    async def execute(self, job: Job, context: Context) -> None:
        """Execute with logging from shared resources."""
        logger.info(f"[LoggingExecutor] Starting job {job.id}")

        try:
            await asyncio.sleep(0.05)
            logger.info(f"[LoggingExecutor] Completed job {job.id}")
        except Exception as e:
            logger.error(f"[LoggingExecutor] Failed job {job.id}: {e}")
            raise


async def example_custom_executor() -> None:
    """Demonstrate custom executor with Context."""
    dsn = os.getenv(
        "DATABASE_URL",
        "postgresql://pgquser:pgqpw@localhost:5432/pgqdb",
    )
    conn = await asyncpg.connect(dsn)
    driver = AsyncpgDriver(conn)

    resources: dict[str, object] = {"logger": logger}
    pgq = PgQueuer(driver, resources=resources)

    @pgq.entrypoint("logged_task", executor_factory=LoggingExecutor)
    async def logged_task(job: Job, context: Context) -> None:
        """Entrypoint executed by custom LoggingExecutor."""
        logger.info(f"Running logged task for job {job.id}")

    logger.info("✓ Custom executor entrypoint registered")
    await conn.close()


async def example_cancellation() -> None:
    """Demonstrate using context.cancellation for graceful shutdown."""
    dsn = os.getenv(
        "DATABASE_URL",
        "postgresql://pgquser:pgqpw@localhost:5432/pgqdb",
    )
    conn = await asyncpg.connect(dsn)
    driver = AsyncpgDriver(conn)

    pgq = PgQueuer(driver)

    @pgq.entrypoint("long_running")
    async def long_running(job: Job, context: Context) -> None:
        """Demonstrates proper cancellation handling."""
        logger.info(f"Starting long-running job {job.id}")

        try:
            with context.cancellation:
                for i in range(10):
                    await asyncio.sleep(0.1)
                    if i % 2 == 0:
                        logger.info(f"Progress: {i * 10}%")
        except asyncio.CancelledError:
            logger.info(f"Job {job.id} cancelled gracefully")
            raise

    logger.info("✓ Cancellation entrypoint registered")
    await conn.close()


async def main() -> None:
    """Run all Context injection examples."""
    logger.info("=" * 70)
    logger.info("Context Parameter Injection Examples")
    logger.info("=" * 70)

    await example_async_with_context()
    await example_sync_with_context()
    await example_without_context()
    await example_custom_executor()
    await example_cancellation()

    logger.info("=" * 70)
    logger.info("All examples completed successfully!")
    logger.info("=" * 70)


if __name__ == "__main__":
    asyncio.run(main())
