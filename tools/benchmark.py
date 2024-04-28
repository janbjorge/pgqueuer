import asyncio
import contextlib
import time
from datetime import timedelta
from typing import Callable, Generator

import asyncpg
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager
from PgQueuer.queries import Queries


@contextlib.contextmanager
def timer() -> (
    Generator[
        Callable[[], float],
        None,
        None,
    ]
):
    enter = time.perf_counter()
    done: float | None = None
    try:
        yield lambda: done - enter if done else time.perf_counter() - enter
    finally:
        done = time.perf_counter()


async def jobs_per_second(pool: asyncpg.Pool) -> float:
    qm = QueueManager(pool)
    seen = 0

    @qm.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        nonlocal seen
        seen += 1
        if job.payload is None:
            qm.alive = False

    with timer() as elapsed:
        await qm.run(timedelta(seconds=0))

    print(seen, round(elapsed(), 3), round(seen / elapsed(), 1))
    return seen / elapsed()


async def benchmark(
    concurrecy: int,
    N: int,
    pool: asyncpg.Pool,
) -> None:
    queries = Queries(pool)

    await queries.clear_log()
    await queries.clear_queue()

    await queries.enqueue(
        ["fetch"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )
    assert sum(x.count for x in await queries.queue_size()) == N

    await queries.enqueue(
        ["fetch"] * concurrecy**2,
        [None] * concurrecy**2,
        [0] * concurrecy**2,
    )

    jobs = [jobs_per_second(pool) for _ in range(concurrecy)]
    results = await asyncio.gather(*jobs)

    print(
        f"Concurrecy: {concurrecy:<3} "
        f"Jobs: {N:<5} "
        f"Jobs per second: {sum(results)/1_000:.1f}k"
    )


async def main() -> None:
    async with asyncpg.create_pool(
        min_size=20,
        max_size=100,
    ) as pool:
        for concurrecy in (1, 2, 3, 10, 20):
            for n_jobs in (100, 500, 1_000, 5_000):
                await benchmark(concurrecy, n_jobs * concurrecy, pool)


if __name__ == "__main__":
    asyncio.run(main())
