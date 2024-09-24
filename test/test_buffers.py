import asyncio
import random
from datetime import timedelta

import pytest

from pgqueuer.buffers import JobStatusLogBuffer
from pgqueuer.helpers import perf_counter_dt
from pgqueuer.models import Job


def job_faker() -> Job:
    dt = perf_counter_dt()
    return Job(
        id=random.choice(range(1_000_000_000)),
        priority=0,
        created=dt,
        heartbeat=dt,
        status="picked",
        entrypoint="foo",
        payload=None,
    )


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_max_size(max_size: int) -> None:
    helper_buffer = []

    async def helper(x: list) -> None:
        helper_buffer.extend(x)

    async with JobStatusLogBuffer(
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

    async with JobStatusLogBuffer(
        max_size=N * 2,
        timeout=timeout,
        flush_callable=helper,
    ) as buffer:
        for _ in range(N):
            await buffer.add((job_faker(), "successful"))
            assert len(helper_buffer) == 0

        await asyncio.sleep(timeout.total_seconds() * 1.1)

    assert len(helper_buffer) == N


# ### Additional Tests ###


@pytest.mark.parametrize("max_size", (2, 3, 5, 64))  # Adjusted to max_size >=2
async def test_job_buffer_flush_on_exit(max_size: int) -> None:
    """
    Test that the buffer flushes all remaining items upon exiting the context,
    even if max_size is not reached and timeout hasn't occurred.
    """
    helper_buffer = []

    async def helper(x: list) -> None:
        helper_buffer.extend(x)

    async with JobStatusLogBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=100),
        flush_callable=helper,
    ) as buffer:
        for _ in range(max_size - 2):
            await buffer.add((job_faker(), "successful"))
            assert len(helper_buffer) == 0

    # After exiting the context, remaining items should be flushed
    assert len(helper_buffer) == max_size - 2


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_multiple_flushes(max_size: int) -> None:
    """
    Test that the buffer can handle multiple flushes when more items than max_size are added.
    """
    helper_buffer = []
    flush_call_count = 0

    async def helper(x: list) -> None:
        nonlocal flush_call_count
        flush_call_count += 1
        helper_buffer.extend(x)

    async with JobStatusLogBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=100),
        flush_callable=helper,
    ) as buffer:
        total_items = max_size * 3  # Add three times the max_size
        for _ in range(total_items):
            await buffer.add((job_faker(), "successful"))

    # Verify that the buffer flushed three times
    assert flush_call_count == 3
    assert len(helper_buffer) == total_items


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_flush_on_exception(max_size: int) -> None:
    """
    Test that the buffer handles exceptions in the flush_callable gracefully and retries.
    """
    helper_buffer = []
    flush_call_count = 0

    async def faulty_helper(x: list) -> None:
        nonlocal flush_call_count
        flush_call_count += 1
        if flush_call_count < 2:
            raise RuntimeError("Simulated flush failure")
        helper_buffer.extend(x)

    async with JobStatusLogBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=0.01),
        flush_callable=faulty_helper,
    ) as buffer:
        for _ in range(max_size):
            await buffer.add((job_faker(), "successful"))

        # Allow time for the flush to be attempted and retried
        await asyncio.sleep(0.02)

    # The first flush should have failed, and the second should have succeeded
    assert flush_call_count == 2
    assert len(helper_buffer) == max_size


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_flush_order(max_size: int) -> None:
    """
    Test that items are flushed in the order they were added.
    """
    helper_buffer = []

    async def helper(x: list) -> None:
        helper_buffer.extend(x)

    async with JobStatusLogBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=100),
        flush_callable=helper,
    ) as buffer:
        items = [(job_faker(), "successful") for _ in range(max_size)]
        for item in items:
            await buffer.add(item)  # type: ignore[arg-type]

    assert helper_buffer == items


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_no_flush_before_timeout(max_size: int) -> None:
    """
    Test that the buffer does not flush before the timeout if max_size is not reached.
    """
    helper_buffer = []

    async def helper(x: list) -> None:
        helper_buffer.extend(x)

    async with JobStatusLogBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=0.1),
        flush_callable=helper,
    ) as buffer:
        for _ in range(max_size - 1):
            await buffer.add((job_faker(), "successful"))
            assert len(helper_buffer) == 0

        # Wait less than the timeout
        await asyncio.sleep(0.05)
        assert len(helper_buffer) == 0

    # After exiting, buffer should flush
    assert len(helper_buffer) == max_size - 1


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_concurrent_adds(max_size: int) -> None:
    """
    Test that the buffer can handle concurrent additions without losing items.
    """
    helper_buffer = []

    async def helper(x: list) -> None:
        helper_buffer.extend(x)

    async with JobStatusLogBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=100),
        flush_callable=helper,
    ) as buffer:

        async def add_items(n: int) -> None:
            for _ in range(n):
                await buffer.add((job_faker(), "successful"))

        tasks = [asyncio.create_task(add_items(max_size // 2)) for _ in range(4)]
        await asyncio.gather(*tasks)

    # Total items added: (max_size // 2) * 4
    expected = (max_size // 2) * 4
    assert len(helper_buffer) == expected


async def test_job_buffer_empty_flush() -> None:
    """
    Test that flushing an empty buffer does not cause any issues.
    """
    helper_buffer = []

    async def helper(x: list) -> None:
        helper_buffer.extend(x)

    async with JobStatusLogBuffer(
        max_size=10,
        timeout=timedelta(seconds=0.1),
        flush_callable=helper,
    ):
        # Do not add any items and let the buffer flush on exit
        pass

    assert len(helper_buffer) == 0


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_reuse_after_flush(max_size: int) -> None:
    """
    Test that the buffer can be reused after a flush has occurred.
    """
    helper_buffer = []

    async def helper(x: list) -> None:
        helper_buffer.extend(x)

    async with JobStatusLogBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=100),
        flush_callable=helper,
    ) as buffer:
        # First flush
        for _ in range(max_size):
            await buffer.add((job_faker(), "successful"))
        assert len(helper_buffer) == max_size

        # Reset helper_buffer for the second flush
        helper_buffer.clear()

        # Second flush
        for _ in range(max_size):
            await buffer.add((job_faker(), "successful"))
        assert len(helper_buffer) == max_size


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_exception_during_flush(max_size: int) -> None:
    """
    Test that the buffer handles exceptions during flush without losing items.
    """
    helper_buffer = []
    flush_call_count = 0

    async def faulty_helper(x: list) -> None:
        nonlocal flush_call_count
        flush_call_count += 1
        if flush_call_count == 1:
            raise RuntimeError("Simulated flush failure")
        helper_buffer.extend(x)

    async with JobStatusLogBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=0.01),
        flush_callable=faulty_helper,
    ) as buffer:
        for _ in range(max_size):
            await buffer.add((job_faker(), "successful"))

        # Allow time for the flush to be attempted and retried
        await asyncio.sleep(0.02)

    # After first failure, flush should retry and succeed
    assert flush_call_count == 2
    assert len(helper_buffer) == max_size


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_flush_callable_called_correctly(max_size: int) -> None:
    """
    Test that the flush_callable is called with the correct items.
    """
    received_items = []

    async def helper(x: list) -> None:
        received_items.extend(x)

    async with JobStatusLogBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=100),
        flush_callable=helper,
    ) as buffer:
        items = [(job_faker(), "successful") for _ in range(max_size)]
        for item in items:
            await buffer.add(item)  # type: ignore[arg-type]

    assert received_items == items
