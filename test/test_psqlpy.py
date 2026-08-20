from __future__ import annotations

import uuid
from datetime import timedelta
from typing import AsyncGenerator

import psqlpy
import pytest_asyncio

from pgqueuer import db
from pgqueuer.queries import EntrypointExecutionParameter, Queries


@pytest_asyncio.fixture(scope="function")
async def psqlpypool(dsn: str) -> AsyncGenerator[psqlpy.ConnectionPool, None]:
    pool = psqlpy.ConnectionPool(dsn=dsn, max_db_pool_size=5)
    try:
        yield pool
    finally:
        pool.close()


@pytest_asyncio.fixture(scope="function")
async def psqlpydriver(
    psqlpypool: psqlpy.ConnectionPool,
) -> AsyncGenerator[db.PsqlpyDriver, None]:
    async with db.PsqlpyDriver(psqlpypool) as driver:
        yield driver


async def test_enqueue_dequeue_roundtrip(psqlpydriver: db.PsqlpyDriver) -> None:
    """The Queries layer round-trips over psqlpy's parameter binding."""
    N = 64
    q = Queries(psqlpydriver)

    await q.enqueue(
        ["fetch"] * N,
        [f"{n}".encode() for n in range(N)],
        [0] * N,
    )
    assert sum(x.count for x in await q.queue_size()) == N

    seen = list[int]()
    while jobs := await q.dequeue(
        batch_size=10,
        entrypoints={"fetch": EntrypointExecutionParameter(0)},
        queue_manager_id=uuid.uuid4(),
        global_concurrency_limit=1000,
        heartbeat_timeout=timedelta(seconds=30),
    ):
        for job in jobs:
            assert job.payload is not None
            seen.append(int(job.payload))
            await q.log_jobs([(job, "successful", None)])

    assert seen == list(range(N))


async def test_binary_payloads_survive_batch_enqueue(psqlpydriver: db.PsqlpyDriver) -> None:
    """Batch enqueue wraps bytea[] members for psqlpy without mangling them."""
    payloads = [b"\x00\x01\x02", b"\xff", bytes(range(256)), None]
    q = Queries(psqlpydriver)

    await q.enqueue(["fetch"] * len(payloads), payloads, [0] * len(payloads))

    rows = await psqlpydriver.fetch(
        f"SELECT payload FROM {q.qbq.qualified.queue_table} ORDER BY id"
    )
    assert [row["payload"] for row in rows] == payloads


async def test_install_uninstall(psqlpydriver: db.PsqlpyDriver) -> None:
    """Multi-statement schema DDL round-trips through psqlpy's simple protocol."""
    q = Queries(psqlpydriver)

    await q.uninstall()
    assert not await q.has_table(q.qbe.settings.queue_table)

    await q.install()
    assert await q.has_table(q.qbe.settings.queue_table)
