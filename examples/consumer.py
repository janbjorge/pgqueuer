from __future__ import annotations

from datetime import datetime

import asyncpg

from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job, Schedule


async def main() -> PgQueuer:
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
