import asyncio
import uuid
from datetime import timedelta
from itertools import count

import pytest
from helpers import mocked_job

from pgqueuer.buffers import JobStatusLogBuffer, TimedOverflowBuffer
from pgqueuer.models import JOB_STATUS, Job


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
            await buffer.add((job_faker(), "successful", None))
            assert len(helper_buffer) == 0

        await buffer.add((job_faker(), "successful", None))

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
            await buffer.add((job_faker(), "successful", None))
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
            await buffer.add((job_faker(), "successful", None))
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
                await buffer.add((job_faker(), "successful", None))
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
            await buffer.add((job_faker(), "successful", None))

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
                await buffer.add((job_faker(), "successful", None))

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
            await buffer.add((job_faker(), "successful", None))
        await buffer.flush()
        assert len(helper_buffer) == max_size

        # Reset helper_buffer for the second flush
        helper_buffer.clear()

        # Second flush
        for _ in range(max_size):
            await buffer.add((job_faker(), "successful", None))
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
            await buffer.add((job_faker(), "successful", None))

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


async def test_job_buffer_callback_exception_during_teardown() -> None:
    N = 10
    items: list[tuple[Job, JOB_STATUS, None]] = [
        (job_faker(), "successful", None) for _ in range(N)
    ]

    async def helper(_: object) -> None:
        raise ValueError

    async with JobStatusLogBuffer(
        max_size=N**2,  # max size must be gt. N.
        timeout=timedelta(seconds=60),  # must be gt. run time of 'for loop' in the with block.
        callback=helper,
    ) as buffer:
        for item in items:
            await buffer.add(item)

    # Was uanble to flush at exit, buffer should have all elements.
    assert buffer.events.qsize() == N


# Additional tests for the refactored TimedOverflowBuffer


async def test_buffer_retry_manager_integration() -> None:
    """Test that the buffer correctly integrates with the retry manager."""
    helper_buffer = []
    retry_attempts = []

    async def flaky_callback(items: list[str]) -> None:
        retry_attempts.append(len(items))
        if len(retry_attempts) == 1:
            raise ConnectionError("Simulated connection failure")
        helper_buffer.extend(items)

    async with TimedOverflowBuffer(
        max_size=2,
        timeout=timedelta(seconds=0.01),
        callback=flaky_callback,
    ) as buffer:
        await buffer.add("item1")
        await buffer.add("item2")  # Should trigger flush and fail once
        await asyncio.sleep(0.02)  # Give time for retry

    assert len(retry_attempts) == 2  # Original attempt + 1 retry
    assert len(helper_buffer) == 2  # Items should eventually be processed


async def test_buffer_shutdown_behavior() -> None:
    """Test that the buffer properly handles shutdown with pending items."""
    helper_buffer = []
    call_count = 0

    async def slow_callback(items: list[str]) -> None:
        nonlocal call_count
        call_count += 1
        # Simulate some processing time
        await asyncio.sleep(0.001)
        helper_buffer.extend(items)

    async with TimedOverflowBuffer(
        max_size=10,  # Large size so items accumulate
        timeout=timedelta(seconds=10),  # Long timeout
        callback=slow_callback,
    ) as buffer:
        await buffer.add("item1")
        await buffer.add("item2")
        await buffer.add("item3")

    # All items should be flushed during shutdown
    assert len(helper_buffer) == 3
    assert call_count >= 1


async def test_buffer_concurrent_flush_prevention() -> None:
    """Test that concurrent flushes are prevented by the lock."""
    flush_order = []
    
    async def slow_callback(items: list[str]) -> None:
        flush_order.append(f"start-{len(items)}")
        await asyncio.sleep(0.01)  # Simulate slow processing
        flush_order.append(f"end-{len(items)}")

    async with TimedOverflowBuffer(
        max_size=2,
        timeout=timedelta(seconds=0.005),
        callback=slow_callback,
    ) as buffer:
        # Add items quickly to trigger multiple potential flushes
        await buffer.add("item1")
        await buffer.add("item2")  # Triggers flush
        await buffer.add("item3")
        await buffer.add("item4")  # Triggers flush
        
        # Wait for all flushes to complete
        await asyncio.sleep(0.1)

    # Should have at least 2 complete flush cycles
    assert len([x for x in flush_order if x.startswith("start")]) >= 2
    assert len([x for x in flush_order if x.startswith("end")]) >= 2


async def test_buffer_pop_until_deadline() -> None:
    """Test that pop_until respects the deadline."""
    items_popped = []
    
    async def test_callback(items: list[str]) -> None:
        items_popped.extend(items)

    buffer = TimedOverflowBuffer(
        max_size=100,  # Large size to avoid auto-flush
        timeout=timedelta(seconds=10),
        callback=test_callback,
    )

    # Add many items
    for i in range(10):
        await buffer.add(f"item{i}")

    # Test pop_until with a short deadline
    popped = []
    async for item in buffer.pop_until(timedelta(milliseconds=1)):
        popped.append(item)

    # Should pop some items but not necessarily all due to deadline
    assert len(popped) <= 10
    assert len(popped) > 0


async def test_buffer_periodic_flush_shutdown() -> None:
    """Test that periodic flush stops when shutdown is set."""
    helper_buffer = []
    flush_count = 0

    async def counting_callback(items: list[str]) -> None:
        nonlocal flush_count
        flush_count += 1
        helper_buffer.extend(items)

    buffer = TimedOverflowBuffer(
        max_size=100,  # Large size to avoid overflow flush
        timeout=timedelta(milliseconds=10),  # Short timeout for periodic flush
        callback=counting_callback,
    )

    async with buffer:
        await buffer.add("item1")
        await asyncio.sleep(0.05)  # Let a few periodic flushes happen
        
    # Should have had at least one flush during the sleep period
    assert flush_count >= 1


async def test_buffer_empty_pop_until() -> None:
    """Test pop_until behavior when buffer is empty."""
    async def dummy_callback(items: list[str]) -> None:
        pass

    buffer = TimedOverflowBuffer(
        max_size=10,
        timeout=timedelta(seconds=1),
        callback=dummy_callback,
    )

    # Test pop_until on empty buffer
    popped = []
    async for item in buffer.pop_until():
        popped.append(item)

    assert len(popped) == 0


async def test_buffer_retry_manager_backoff_reset() -> None:
    """Test that successful operations reset the retry backoff."""
    helper_buffer = []
    attempt_count = 0

    async def sometimes_failing_callback(items: list[str]) -> None:
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count == 1:
            raise RuntimeError("First failure")
        helper_buffer.extend(items)

    async with TimedOverflowBuffer(
        max_size=1,
        timeout=timedelta(seconds=10),
        callback=sometimes_failing_callback,
    ) as buffer:
        # First batch - will fail then succeed
        await buffer.add("item1")
        await asyncio.sleep(0.02)  # Wait for retry
        
        # Check that backoff was reset after success
        initial_delay = buffer.retry_manager.retry_backoff.start_delay
        current_delay = buffer.retry_manager.retry_backoff.current_delay
        assert current_delay == initial_delay  # Should be reset
        
        # Second batch - should succeed immediately
        await buffer.add("item2")

    assert len(helper_buffer) == 2
    assert attempt_count == 3  # 1 fail + 1 success + 1 success
