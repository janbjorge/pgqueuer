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

    @qm.entrypoint("async")
    async def asyncfetch(job: Job) -> None:
        nonlocal seen
        seen += 1
        if job.payload is None:
            qm.alive = False

    @qm.entrypoint("sync")
    def syncfetch(job: Job) -> None:
        nonlocal seen
        seen += 1
        if job.payload is None:
            qm.alive = False

    with timer() as elapsed:
        await qm.run(timedelta(seconds=0))

    return seen / elapsed()


async def benchmark(
    entrypoint: str,
    concurrecy: int,
    N: int,
    pool: asyncpg.Pool,
) -> None:
    queries = Queries(pool)
    await queries.enqueue(
        [entrypoint] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )

    await queries.enqueue(
        [entrypoint] * concurrecy**2,
        [None] * concurrecy**2,
        [0] * concurrecy**2,
    )

    jps = sum(
        await asyncio.gather(
            *[jobs_per_second(pool) for _ in range(concurrecy)],
        )
    )

    print(
        f"Entrypoint: {entrypoint:<5} ",
        f"Concurrecy: {concurrecy:<2} ",
        f"Jobs: {N:<5} ",
        f"JPS: {jps/1_000:.1f}k",
    )


async def main() -> None:
    async with asyncpg.create_pool(
        min_size=20,
        max_size=99,
    ) as pool:
        queries = Queries(pool)
        await queries.clear_log()
        await queries.clear_queue()
        for entrypoint in ("sync", "async"):
            for concurrecy in range(1, 6):
                await benchmark(entrypoint, concurrecy, concurrecy * 1_000, pool)
                await asyncio.sleep(0)


if __name__ == "__main__":
    asyncio.run(main())
