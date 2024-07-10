import asyncpg
from PgQueuer.db import AsyncpgDriver, dsn
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager


async def main() -> QueueManager:
    connection = await asyncpg.connect(dsn())
    driver = AsyncpgDriver(connection)
    qm = QueueManager(driver)

    # Setup the 'fetch' entrypoint
    @qm.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed message: {job}")

    N = 1_000
    # Enqueue jobs.
    await qm.queries.enqueue(
        ["fetch"] * N,
        [f"this is from me: {n}".encode() for n in range(N)],
        [0] * N,
    )
    return qm
