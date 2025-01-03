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
    # setup
    logger.info("setup")
    try:
        pgq = await create_pgqueuer()
        yield pgq
    finally:
        # teardown
        logger.info("teardown")
