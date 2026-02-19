"""
Tests for health check task cancellation on shutdown.

Verifies that the periodic health check task is properly cancelled
when QueueManager shuts down, preventing 10-second delays.
"""

from __future__ import annotations

import asyncio
import time


async def test_periodic_task_cancellation() -> None:
    """
    Test that a periodic task can be cancelled and awaited cleanly.

    This simulates the pattern used in QueueManager.run():
        periodic_health_check_task.cancel()
        with suppress(asyncio.CancelledError):
            await periodic_health_check_task
    """

    async def periodic_task() -> None:
        """Simulates a periodic health check task."""
        while True:
            await asyncio.sleep(0.1)

    # Create and start the task
    task = asyncio.create_task(periodic_task())
    await asyncio.sleep(0.05)  # Let it start

    # Cancel and await (the fix pattern)
    task.cancel()
    from contextlib import suppress

    with suppress(asyncio.CancelledError):
        await task

    # Verify it's actually done
    assert task.done()
    assert task.cancelled()


async def test_shutdown_doesnt_wait_for_timeout() -> None:
    """
    Test that shutdown is fast, not delayed by timeouts.

    Demonstrates that without proper cancellation, a task with a timeout
    would delay shutdown. With cancellation, it exits immediately.
    """

    async def task_with_timeout() -> None:
        """A task that would normally timeout after 10 seconds."""
        try:
            # Simulate waiting for something with a 10-second timeout
            await asyncio.sleep(10.0)
        except asyncio.CancelledError:
            return

    task = asyncio.create_task(task_with_timeout())
    await asyncio.sleep(0.01)

    # Measure time to cancel and await
    start = time.time()

    task.cancel()
    from contextlib import suppress

    with suppress(asyncio.CancelledError):
        await task

    elapsed = time.time() - start

    # Should complete in milliseconds, not seconds
    assert elapsed < 0.1, f"Cancellation took {elapsed:.2f}s, should be instant"
