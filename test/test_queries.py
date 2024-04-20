import asyncio
import itertools

import asyncpg
import pytest
from PgQueuer import models, queries


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queries_put(pgpool: asyncpg.Pool, N: int) -> None:
    q = queries.PgQueuerQueries(pgpool)

    assert sum((await q.qsize()).values()) == 0

    for _ in range(N):
        await q.put("<placeholer>", None)

    assert sum((await q.qsize()).values()) == N


@pytest.mark.parametrize("N", (1, 2, 64))
@pytest.mark.parametrize("batchsize", (1, 2, 64))
async def test_queries_next_jobs(
    pgpool: asyncpg.Pool,
    N: int,
    batchsize: int,
) -> None:
    q = queries.PgQueuerQueries(pgpool)

    for n in range(N):
        await q.put("<placeholer>", None)

    jobs = list[models.Job]()
    while (next_jobs := await q.next_jobs(batch=batchsize)).root:
        jobs.extend(next_jobs.root)

    assert len(jobs) == N


@pytest.mark.parametrize("N", (1, 2, 64, 256))
@pytest.mark.parametrize("concurrency", (1, 2, 4, 16))
async def test_queries_next_jobs_concurrent(
    pgpool: asyncpg.Pool,
    N: int,
    concurrency: int,
) -> None:
    q = queries.PgQueuerQueries(pgpool)
    assert pgpool.get_max_size() >= concurrency

    for n in range(N):
        await q.put("<placeholer>", f"{n}".encode())

    async def consumer() -> list[int]:
        jobs = list[models.Job]()
        while (next_jobs := await q.next_jobs()).root:
            jobs.extend(next_jobs.root)

        return [int(job.payload or b"") for job in jobs]

    results = itertools.chain.from_iterable(
        await asyncio.gather(*[consumer() for _ in range(concurrency)])
    )

    assert sorted(results) == list(range(N))


async def test_queries_clear(pgpool: asyncpg.Pool) -> None:
    q = queries.PgQueuerQueries(pgpool)
    await q.clear()
    assert sum((await q.qsize()).values()) == 0

    await q.put("<placeholer>", None)
    assert sum((await q.qsize()).values()) == 1

    await q.clear()
    assert sum((await q.qsize()).values()) == 0


@pytest.mark.parametrize("N", (1, 2, 64, 256))
async def test_move_job_log(
    pgpool: asyncpg.Pool,
    N: int,
) -> None:
    q = queries.PgQueuerQueries(pgpool)
    ql = queries.PgQueuerLogQueries(pgpool)

    for n in range(N):
        await q.put("<placeholer>", f"{n}".encode())

    jobs = list[models.Job]()
    while (next_jobs := await q.next_jobs()).root:
        jobs.extend(next_jobs.root)

    for job in jobs:
        await ql.move_job_log(job, status="successful")

    assert await ql.qsize() == {("successful", "<placeholer>", 0): N}
