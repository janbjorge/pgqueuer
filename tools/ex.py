import asyncio

import asyncpg
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager


async def main() -> None:
    pool = await asyncpg.create_pool(min_size=2)

    # mypy wtf?
    assert isinstance(pool, asyncpg.Pool)

    qm = QueueManager(pool)

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

    await qm.run()


if __name__ == "__main__":
    asyncio.run(main())
