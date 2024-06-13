import asyncio
import random
from datetime import datetime, timedelta

import pytest
from PgQueuer.buffers import JobBuffer, _perf_counter_dt
from PgQueuer.models import Job
from PgQueuer.tm import TaskManager


def job_faker() -> Job:
    return Job(
        id=random.sample(range(1_000_000_000), k=1)[0],
        priority=0,
        created=_perf_counter_dt(),
        status="picked",
        entrypoint="foo",
        payload=None,
    )


async def test_perf_counter_dt() -> None:
    assert isinstance(_perf_counter_dt(), datetime)
    assert _perf_counter_dt().tzinfo is not None


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_max_size(max_size: int) -> None:
    helper_buffer = []

    async def helper(x: list) -> None:
        helper_buffer.extend(x)

    buffer = JobBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=100),
        flush_callback=helper,
    )

    for _ in range(max_size - 1):
        await buffer.add_job(job_faker(), "successful")
        assert len(helper_buffer) == 0

    await buffer.add_job(job_faker(), "successful")
    assert len(helper_buffer) == max_size


@pytest.mark.parametrize("N", (5, 64))
@pytest.mark.parametrize("timeout", (timedelta(seconds=0.01), timedelta(seconds=0.001)))
async def test_job_buffer_timeout(N: int, timeout: timedelta) -> None:
    async with TaskManager() as tm:
        helper_buffer = []

        async def helper(x: list) -> None:
            helper_buffer.extend(x)

        buffer = JobBuffer(
            max_size=N * 2,
            timeout=timeout,
            flush_callback=helper,
        )
        tm.add(asyncio.create_task(buffer.monitor()))

        for _ in range(N):
            await buffer.add_job(job_faker(), "successful")
            assert len(helper_buffer) == 0

        await asyncio.sleep(timeout.total_seconds() * 1.1)
        assert len(helper_buffer) == N
        buffer.alive = False
