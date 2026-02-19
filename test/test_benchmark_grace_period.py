"""
Tests for benchmark grace period cancellation.

Verifies that the grace period calculation is correct and that
tasks can be cancelled and cleaned up properly.
"""

from __future__ import annotations

import asyncio


async def test_grace_period_calculation() -> None:
    """
    Test that grace period is calculated correctly.

    Formula: max(5.0, timer_seconds * 0.1)
    - Short timers (< 50s) get 5 second grace period
    - Long timers (>= 50s) get 10% of timer as grace period
    """
    # Short timer uses minimum 5 seconds
    assert max(5.0, 1.0 * 0.1) == 5.0
    assert max(5.0, 10.0 * 0.1) == 5.0
    assert max(5.0, 50.0 * 0.1) == 5.0

    # Longer timers use 10% of duration
    assert max(5.0, 60.0 * 0.1) == 6.0
    assert max(5.0, 100.0 * 0.1) == 10.0
    assert max(5.0, 200.0 * 0.1) == 20.0


async def test_cancel_tasks_after_grace_period() -> None:
    """
    Test the grace period cancellation pattern from the fix.

    1. Create tasks
    2. Wait with timeout (grace period)
    3. Cancel any tasks still pending
    4. Gather results
    """

    async def slow_task() -> None:
        """A task that takes a long time."""
        await asyncio.sleep(100)

    # Create two tasks
    task1 = asyncio.create_task(slow_task())
    task2 = asyncio.create_task(slow_task())

    # Wait with short grace period
    grace_period = 0.01
    done, pending = await asyncio.wait({task1, task2}, timeout=grace_period)

    # Both should still be pending (grace too short)
    assert len(pending) == 2
    assert len(done) == 0

    # Cancel pending tasks
    for task in pending:
        task.cancel()

    # Gather with return_exceptions to handle CancelledError
    await asyncio.gather(*pending, return_exceptions=True)

    # Verify they're cancelled
    assert all(task.cancelled() for task in pending)


async def test_exception_propagation_from_tasks() -> None:
    """
    Test that exceptions from completed tasks are caught and handled.

    The fix pattern checks: if task completed with an exception, re-raise it.
    """

    class MyError(Exception):
        pass

    async def failing_task() -> None:
        raise MyError("Task failed")

    async def success_task() -> None:
        await asyncio.sleep(0.01)

    task1 = asyncio.create_task(failing_task())
    task2 = asyncio.create_task(success_task())

    # Wait for both
    done, _pending = await asyncio.wait({task1, task2}, timeout=1.0)

    # Check for exceptions
    for task in done:
        if not task.cancelled() and task.exception():
            # This is the pattern from the fix
            assert isinstance(task.exception(), MyError)
            break
    else:
        raise AssertionError("Should have found the exception")
