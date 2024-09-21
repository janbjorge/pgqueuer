import asyncio
import random
from datetime import timedelta

import pytest

from pgqueuer.buffers import TimedOverflowBuffer
from pgqueuer.helpers import perf_counter_dt
from pgqueuer.models import STATUS_LOG, Job


class TestBuff(TimedOverflowBuffer[tuple[Job, STATUS_LOG]]): ...


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
async def test_job_buffer_max_size(max_size: int) -> None:
    helper_buffer = []

    async def helper(x: list) -> None:
        helper_buffer.extend(x)

    async with TestBuff(
        max_size=max_size,
        timeout=timedelta(seconds=100),
        flush_callable=helper,
    ) as buffer:
        for _ in range(max_size - 1):
            await buffer.add((job_faker(), "successful"))
            assert len(helper_buffer) == 0

        await buffer.add((job_faker(), "successful"))
        assert len(helper_buffer) == max_size


@pytest.mark.parametrize("N", (5, 64))
@pytest.mark.parametrize("timeout", (timedelta(seconds=0.01), timedelta(seconds=0.001)))
async def test_job_buffer_timeout(
    N: int,
    timeout: timedelta,
) -> None:
    helper_buffer = []

    async def helper(x: list) -> None:
        helper_buffer.extend(x)

    async with TimedOverflowBuffer(
        max_size=N * 2,
        timeout=timeout,
        flush_callable=helper,
    ) as buffer:
        for _ in range(N):
            await buffer.add((job_faker(), "successful"))
            assert len(helper_buffer) == 0

        await asyncio.sleep(timeout.total_seconds() * 1.1)

    assert len(helper_buffer) == N
