import asyncio

import asyncpg

from pgqueuer.db import AsyncpgDriver, dsn
from pgqueuer.models import Schedule
from pgqueuer.sm import SchedulerManager


async def create_scheduler() -> SchedulerManager:
    # Establish a connection to PostgreSQL
    connection = await asyncpg.connect(dsn())
    driver = AsyncpgDriver(connection)
    sm = SchedulerManager(driver)

    # Define and register recurring tasks using cron expressions
    # The cron expression "* * * * *" means the task will run every minute
    @sm.schedule("update_product_catalog", "* * * * *")
    async def update_product_catalog(schedule: Schedule) -> None:
        print(f"Running update_product_catalog task: {schedule}")
        await asyncio.sleep(0.1)
        print("update_product_catalog task completed.")

    # The cron expression "0 0 * * *" means the task will run every day at midnight
    @sm.schedule("clean_expired_tokens", "0 0 * * *")
    async def clean_expired_tokens(schedule: Schedule) -> None:
        print(f"Running clean_expired_tokens task: {schedule}")
        await asyncio.sleep(0.2)
        print("clean_expired_tokens task completed.")

    return sm
