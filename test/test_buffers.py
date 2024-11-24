import asyncio
import uuid
from datetime import timedelta
from itertools import count

import pytest
from helpers import mocked_job

from pgqueuer.buffers import JobStatusLogBuffer
from pgqueuer.helpers import utc_now
from pgqueuer.models import Job


def job_faker(
    cnt: count = count(),
    queue_manager_id: uuid.UUID = uuid.uuid4(),
) -> Job:
    return mocked_job(
        id=next(cnt),
        status="picked",
        entrypoint="foo",
        priority=0,
        payload=None,
        queue_manager_id=queue_manager_id,
    )


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_max_size(max_size: int) -> None:
    helper_buffer = []

    async def helper(x: list) -> None:
        helper_buffer.extend(x)

    async with JobStatusLogBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=100),
        callback=helper,
    ) as buffer:
        for _ in range(max_size - 1):
            await buffer.add((job_faker(), "successful"))
            assert len(helper_buffer) == 0

        await buffer.add((job_faker(), "successful"))

    # On ctx-mangner exit flush is forced.
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
        callback=helper,
    ) as buffer:
        for _ in range(N):
            await buffer.add((job_faker(), "successful"))
            assert len(helper_buffer) == 0

        await asyncio.sleep(timeout.total_seconds() * 1.1)

    assert len(helper_buffer) == N


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
        callback=helper,
    ) as buffer:
        for _ in range(max_size - 2):
            await buffer.add((job_faker(), "successful"))
            assert len(helper_buffer) == 0

    # After exiting the context, remaining items should be flushed
    assert len(helper_buffer) == max_size - 2


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
@pytest.mark.parametrize("flushes", (1, 2, 3, 5, 64))
async def test_job_buffer_multiple_flushes(max_size: int, flushes: int) -> None:
    """
    Test that the buffer can handle multiple flushes when more items than max_size are added.
    """
    helper_buffer = []

    async def helper(x: list) -> None:
        helper_buffer.append(x)

    async with JobStatusLogBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=100),
        callback=helper,
    ) as buffer:
        for _ in range(flushes):
            for _ in range(max_size):
                await buffer.add((job_faker(), "successful"))
            buffer.next_flush = utc_now()
            await buffer.flush()

    # Verify that the buffer flushed three times
    assert len(helper_buffer) == flushes


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_flush_on_exception(max_size: int) -> None:
    """
    Test that the buffer handles exceptions in the callback gracefully and retries.
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
        callback=faulty_helper,
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
        callback=helper,
    ) as buffer:
        items = [(job_faker(), "successful") for _ in range(max_size)]
        for item in items:
            await buffer.add(item)  # type: ignore[arg-type]

    assert helper_buffer == items


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
        callback=helper,
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
        callback=helper,
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
        callback=helper,
    ) as buffer:
        # First flush
        for _ in range(max_size):
            await buffer.add((job_faker(), "successful"))
        buffer.next_flush = utc_now()
        await buffer.flush()
        assert len(helper_buffer) == max_size

        # Reset helper_buffer for the second flush
        helper_buffer.clear()

        # Second flush
        for _ in range(max_size):
            await buffer.add((job_faker(), "successful"))
        buffer.next_flush = utc_now()
        await buffer.flush()
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
        callback=faulty_helper,
    ) as buffer:
        for _ in range(max_size):
            await buffer.add((job_faker(), "successful"))

        # Allow time for the flush to be attempted and retried
        await asyncio.sleep(0.02)

    # After first failure, flush should retry and succeed
    assert flush_call_count == 2
    assert len(helper_buffer) == max_size


@pytest.mark.parametrize("max_size", (1, 2, 3, 5, 64))
async def test_job_buffer_callback_called_correctly(max_size: int) -> None:
    """
    Test that the callback is called with the correct items.
    """
    items = [(job_faker(), "successful") for _ in range(max_size)]
    received_items = []

    async def helper(x: list) -> None:
        received_items.extend(x)

    async with JobStatusLogBuffer(
        max_size=max_size,
        timeout=timedelta(seconds=100),
        callback=helper,
    ) as buffer:
        for item in items:
            await buffer.add(item)  # type: ignore[arg-type]

    assert received_items == items
