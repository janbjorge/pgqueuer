import asyncio
import time

import asyncpg
import pytest
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager


async def jobs_per_second(
    N: int,
    pool: asyncpg.Pool,
) -> float:
    qm = QueueManager(pool)

    @qm.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        if job.payload is None:
            qm.alive = False

    enter = time.perf_counter()
    await qm.run()
    done = time.perf_counter()
    return N / (done - enter)


@pytest.mark.parametrize("N", (100, 500, 1_000))
@pytest.mark.parametrize("concurrecy", (1, 2, 3, 4, 5))
async def test_benchmark(
    concurrecy: int,
    N: int,
    pgpool: asyncpg.Pool,
) -> None:
    qm = QueueManager(pgpool)

    for n in range(N):
        await qm.queries.enqueue("fetch", f"{n}".encode())

    # Hacky, must fine a bether way to shutdown.
    for n in range(concurrecy**2):
        await qm.queries.enqueue("fetch", None)

    jobs = [jobs_per_second(N, pgpool) for _ in range(concurrecy)]
    jps = sum(await asyncio.wait_for(asyncio.gather(*jobs), timeout=30))

    print(f"Concurrecy: {concurrecy} Jobs: {N}, Jobs per second: {jps:.1f}")
