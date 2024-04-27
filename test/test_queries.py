import asyncio

import asyncpg
import pytest
from PgQueuer import queries


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queries_put(pgpool: asyncpg.Pool, N: int) -> None:
    q = queries.Queries(pgpool)

    assert sum((await q.queue_size()).values()) == 0

    for _ in range(N):
        await q.enqueue("placeholer", None)

    assert sum((await q.queue_size()).values()) == N


@pytest.mark.parametrize("N", (1, 2, 64))
async def test_queries_next_jobs(
    pgpool: asyncpg.Pool,
    N: int,
) -> None:
    q = queries.Queries(pgpool)

    for n in range(N):
        await q.enqueue("placeholer", f"{n}".encode())

    seen = list[int]()
    while job := await q.dequeue():
        payoad = job.payload
        assert payoad is not None
        seen.append(int(payoad))
        await q.log_job(job, "successful")

    assert seen == list(range(N))


@pytest.mark.parametrize("N", (1, 2, 64, 256))
@pytest.mark.parametrize("concurrency", (1, 2, 4, 16))
async def test_queries_next_jobs_concurrent(
    pgpool: asyncpg.Pool,
    N: int,
    concurrency: int,
) -> None:
    assert pgpool.get_max_size() >= concurrency
    q = queries.Queries(pgpool)

    for n in range(N):
        await q.enqueue("placeholer", f"{n}".encode())

    seen = list[int]()

    async def consumer() -> None:
        while job := await q.dequeue():
            payload = job.payload
            assert payload is not None
            seen.append(int(payload))
            await q.log_job(job, "successful")

    await asyncio.wait_for(
        asyncio.gather(*[consumer() for _ in range(concurrency)]),
        10,
    )

    assert sorted(seen) == list(range(N))


async def test_queries_clear(pgpool: asyncpg.Pool) -> None:
    q = queries.Queries(pgpool)
    await q.clear_queue()
    assert sum((await q.queue_size()).values()) == 0

    await q.enqueue("placeholer", None)
    assert sum((await q.queue_size()).values()) == 1

    await q.clear_queue()
    assert sum((await q.queue_size()).values()) == 0


@pytest.mark.parametrize("N", (1, 2, 64, 256))
async def test_move_job_log(
    pgpool: asyncpg.Pool,
    N: int,
) -> None:
    q = queries.Queries(pgpool)

    for n in range(N):
        await q.enqueue("placeholer", f"{n}".encode())

    while next_job := await q.dequeue():
        await q.log_job(next_job, status="successful")

    assert await q.log_size() == {("successful", "placeholer", 0): N}
