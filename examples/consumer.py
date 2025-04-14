from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncGenerator

import asyncpg

from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.logconfig import logger
from pgqueuer.models import Job, Schedule


async def create_pgqueuer() -> PgQueuer:
    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)
    pgq = PgQueuer(driver)

    # Setup the 'fetch' entrypoint
    @pgq.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job!r}")

    @pgq.schedule("scheduled_every_minute", "* * * * *")
    async def scheduled_every_minute(schedule: Schedule) -> None:
        print(f"Executed every minute {schedule!r} {datetime.now()!r}")

    return pgq


@asynccontextmanager
async def main() -> AsyncGenerator[PgQueuer, None]:
    """
    A context manager for setting up and tearing down the PgQueuer instance.

    This function manages the lifecycle of the PgQueuer instance, ensuring proper setup and teardown
    when used in an asynchronous context. It includes the following steps:

    Setup:
        - Logs the start of the setup process.
        - Creates and configures a PgQueuer instance by connecting to the database and initializing
          entrypoints and schedules.

    Teardown:
        - Logs the start of the teardown process.
        - Ensures cleanup actions for the PgQueuer instance, releasing resources like database
          connections.

    Yields:
        PgQueuer: The configured instance ready for processing jobs and schedules.
    """
    logger.info("setup")
    try:
        pgq = await create_pgqueuer()
        yield pgq
    finally:
        logger.info("teardown")
