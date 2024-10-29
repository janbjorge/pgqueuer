from __future__ import annotations

import asyncpg

from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager


async def main() -> QueueManager:
    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)
    qm = QueueManager(driver)

    # Setup the 'fetch' entrypoint
    @qm.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job}")

    return qm
