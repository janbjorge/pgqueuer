"""
Helper functions for time measurement and asynchronous event handling.

This module provides utility functions for obtaining high-resolution timestamps
and waiting for events from asynchronous queues with specified timeouts.
It is used to assist in measuring performance and managing asynchronous tasks
that involve timing constraints.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import datetime, timedelta, timezone

from . import listeners, models


def perf_counter_dt() -> datetime:
    """
    Get the current high-resolution time as a timezone-aware datetime object.

    This function uses `time.perf_counter()` to retrieve the current time
    with the highest available resolution, suitable for measuring short durations.
    The timestamp is then converted to a timezone-aware `datetime` object
    in UTC.

    Returns:
        datetime: The current high-resolution time as a datetime object in UTC timezone.
    """
    return datetime.fromtimestamp(time.perf_counter(), tz=timezone.utc)


def wait_for_notice_event(
    queue: listeners.PGNoticeEventListener,
    timeout: timedelta,
) -> asyncio.Task[models.TableChangedEvent | None]:
    """
    Wait for a table change event from a PostgreSQL notice event queue with a timeout.

    Creates and returns an asyncio Task that waits for an event from the provided
    `PGNoticeEventListener` queue. If an event is received within the specified
    timeout duration, the task completes and returns the event.
    If the timeout expires before an event is received, the task completes and
    returns `None` without raising a `TimeoutError`.

    Args:
        queue (listeners.PGNoticeEventListener): The queue to listen on for events.
        timeout (timedelta): The maximum duration to wait for an event.

    Returns:
        asyncio.Task[models.TableChangedEvent | None]: An asyncio Task that resolves
            to the event if received, or `None` if the timeout expires.
    """

    async def suppressed_timeout() -> models.TableChangedEvent | None:
        with contextlib.suppress(asyncio.TimeoutError):
            return await asyncio.wait_for(
                queue.get(),
                timeout=timeout.total_seconds(),
            )
        return None

    return asyncio.create_task(suppressed_timeout())


def retry_timer_buffer_timeout(
    dts: list[timedelta],
    *,
    _default: timedelta = timedelta(hours=24),
    _t0: timedelta = timedelta(seconds=0),
) -> timedelta:
    try:
        return min(dt for dt in dts if dt > _t0)
    except ValueError:
        return _default
