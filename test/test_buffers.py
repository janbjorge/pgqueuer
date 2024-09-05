import asyncio
import random
from datetime import timedelta

import pytest

from pgqueuer.buffers import JobBuffer
from pgqueuer.db import Driver
from pgqueuer.helpers import perf_counter_dt
from pgqueuer.models import Job
from pgqueuer.queries import Queries
from pgqueuer.tm import TaskManager


def job_faker() -> Job:
    return Job(
        id=random.choice(range(1_000_000_000)),
        priority=0,
        created=perf_counter_dt(),
        status="picked",
        entrypoint="foo",
        payload=None,
    )


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_max_size(
    max_size: int,
    pgdriver: Driver,
) -> None:
    helper_buffer = []

    async def helper(x: list) -> None:
        helper_buffer.extend(x)

    queries = Queries(pgdriver)
    queries.log_jobs = helper  # type: ignore

    buffer = JobBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=100),
        queries=queries,
    )

    for _ in range(max_size - 1):
        await buffer.add_job(job_faker(), "successful")
        assert len(helper_buffer) == 0

    await buffer.add_job(job_faker(), "successful")
    assert len(helper_buffer) == max_size


@pytest.mark.parametrize("N", (5, 64))
@pytest.mark.parametrize("timeout", (timedelta(seconds=0.01), timedelta(seconds=0.001)))
async def test_job_buffer_timeout(
    N: int,
    timeout: timedelta,
    pgdriver: Driver,
) -> None:
    async with TaskManager() as tm:
        helper_buffer = []

        async def helper(x: list) -> None:
            helper_buffer.extend(x)

        queries = Queries(pgdriver)
        queries.log_jobs = helper  # type: ignore

        buffer = JobBuffer(
            max_size=N * 2,
            timeout=timeout,
            queries=queries,
        )
        tm.add(asyncio.create_task(buffer.monitor()))

        for _ in range(N):
            await buffer.add_job(job_faker(), "successful")
            assert len(helper_buffer) == 0

        await asyncio.sleep(timeout.total_seconds() * 1.1)
        assert len(helper_buffer) == N
        buffer.alive.set()
