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
import random
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Callable, Generator
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from croniter import croniter

from . import listeners, models


@dataclass
class ExponentialBackoff:
    """
    A utility class for calculating exponential backoff delays.

    Attributes:
        start_delay (float): The starting delay for the backoff.
        multiplier (float): The factor by which the delay increases on each step.
        max_delay (timedelta): The maximum sleep duration allowed in the backoff sequence.
        current_delay (float): The current delay in the backoff sequence.
    """

    start_delay: timedelta = field(default=timedelta(seconds=0.01))
    multiplier: float = field(default=2)
    max_delay: timedelta = field(default=timedelta(seconds=10))
    current_delay: timedelta = field(init=False)

    def __post_init__(self) -> None:
        """
        Initialize the delay to the starting delay value.
        """
        if self.multiplier <= 1:
            raise ValueError(f"Multiplier must be greater than 1, but got {self.multiplier}.")
        self.current_delay = self.start_delay

    def next_delay(self) -> timedelta:
        """
        Calculate and return the next delay in the backoff sequence.

        Returns:
            float: The updated delay value, capped at max_limit.
        """
        self.current_delay = min(self.current_delay * self.multiplier, self.max_delay)
        return self.current_delay

    def reset(self) -> None:
        """
        Reset the delay to the starting delay value.
        """
        self.current_delay = self.start_delay


@contextlib.contextmanager
def timer() -> Generator[Callable[[], timedelta], None, None]:
    """
    Context manager to measure elapsed time.

    Yields a callable that returns the elapsed time as a timedelta.
    """
    enter = datetime.now()
    exit: None | datetime = None
    try:
        yield lambda: (exit or datetime.now()) - enter
    finally:
        exit = datetime.now()


def utc_now() -> datetime:
    """
    Get the current UTC time as a timezone-aware datetime object.

    This function returns the current time using `datetime.now()`,
    with UTC as the specified timezone, ensuring the result is
    timezone-aware.
    """
    return datetime.now(timezone.utc)


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
    """
    Returns the smallest timedelta from the input list `dts` that is greater than `_t0`.
    If no such timedelta exists, returns `_default`.

    Parameters:
        dts (list[timedelta]): A list of timedelta objects to evaluate.
        _default (timedelta, optional): The fallback value returned if no valid timedelta is found.
            Defaults to 24 hours.
        _t0 (timedelta, optional): The minimum threshold timedelta. Defaults to 0 seconds.

    Returns:
        timedelta: The smallest valid timedelta from the list or `_default` if none found.
    """
    try:
        return min(dt for dt in dts if dt > _t0)
    except ValueError:
        return _default


def timeout_with_jitter(
    timeout: timedelta,
    jitter_span: tuple[float, float] = (0.8, 1.2),
) -> timedelta:
    """
    Calculate a delay with a jitter applied.

    Args:
        base_timeout (timedelta): The base timeout as a timedelta object.
        jitter_span (tuple[float, float]): A tuple representing the lower and upper
            bounds of the jitter range.

    Returns:
        float: The calculated delay with jitter applied, in seconds.
        The jitter will be in the specified range of the base delay.
    """
    return timeout * random.uniform(*jitter_span)


def normalize_cron_expression(expression: str) -> str:
    """Expands a cron expression into its component parts as a space-separated string.

    Args:
        expression (str): A cron expression or shorthand to expand.

    Returns:
        str: Expanded cron fields as a space-separated string.

    Example:
        >>> expand_cron_expressions("@hourly")
        '0 * * * *'
    """
    return " ".join(croniter(expression).expressions)


def add_schema_to_dsn(dsn: str, schema: str) -> str:
    """
    Add a search_path schema to a PostgreSQL DSN, raising if one already exists.

    Args:
        dsn (str): The PostgreSQL DSN (e.g., "postgresql://user:password@host:port/dbname").
        schema (str): The schema to set as the search_path.

    Returns:
        str: The updated DSN with the schema included in the options parameter.

    Raises:
        ValueError: If a search_path already exists in the options.
    """
    parts = urlparse(dsn)
    query = parse_qs(parts.query)
    options = query.get("options", [])

    # Check for an existing search_path
    if any(opt.startswith("-c search_path=") for opt in options):
        raise ValueError("search_path is already set in the options parameter.")

    options.append(f"-csearch_path={schema}")
    query["options"] = options

    return urlunparse(parts._replace(query=urlencode(query, doseq=True)))
