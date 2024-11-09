from __future__ import annotations

import asyncio

import asyncpg
import icecream

from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Schedule
from pgqueuer.scheduler import Scheduler


async def main() -> Scheduler:
    icecream.ic("booting....")

    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)
    sc = Scheduler(driver)

    # Setup the 'fetch' entrypoint
    @sc.schedule("fetch_db", "* * * * *")
    @sc.schedule("fetch_db", "*/1 * * * *")
    @sc.schedule("fetch_db", "*/2 * * * *")
    @sc.schedule("fetch_db", "*/3 * * * *")
    async def fetch_db(schedule: Schedule) -> None:
        icecream.ic("Fetch db", schedule)
        await asyncio.sleep(0.1)
        icecream.ic("Fetch db - done")

    @sc.schedule("fetch_vg", "* * * * *")
    @sc.schedule("fetch_vg", "*/1 * * * *")
    @sc.schedule("fetch_vg", "*/2 * * * *")
    @sc.schedule("fetch_vg", "*/3 * * * *")
    async def fetch_vg(schedule: Schedule) -> None:
        icecream.ic("Fetch vg", schedule)
        await asyncio.sleep(0.1)
        icecream.ic("Fetch vg - done")

    icecream.ic(sc.registry)

    return sc
