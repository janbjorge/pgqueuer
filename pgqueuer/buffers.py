"""
Module for buffering items and their statuses before processing.
This module defines the `TimedOverflowBuffer` class, which accumulates items and their statuses
until either a specified capacity is reached or a timeout occurs. It then flushes the
buffer by invoking a provided asynchronous callable. This buffering mechanism helps reduce the
number of processing operations by batching them, improving performance.
"""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import datetime, timedelta
from typing import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Generic,
    TypeVar,
)

from typing_extensions import Self

from . import helpers, logconfig, models

T = TypeVar("T")


@dataclasses.dataclass
class TimedOverflowBuffer(Generic[T]):
    """
    Accumulates items, flushing them based on timeouts or buffer capacity.

    The `TimedOverflowBuffer` class collects items in a buffer. It flushes the
    buffer when either the maximum number of items (`max_size`) is reached or a specified
    timeout (`timeout`) has elapsed since the last flush. The flush operation involves
    invoking the provided asynchronous callable with the accumulated items.

    Attributes:
        max_size (int): The maximum number of items to buffer before flushing.
        timeout (timedelta): The maximum duration to wait before flushing the buffer,
            regardless of size.
        flush_callable (Callable[[list[T]], Awaitable[None]]): The asynchronous
            callable used to flush items, such as logging to a database.
        alive (asyncio.Event): An event to signal when the buffer should stop monitoring
            (e.g., during shutdown).
        events (asyncio.Queue[T]): An asynchronous queue holding the buffered items.
        lock (asyncio.Lock): A lock to ensure thread safety during flush operations.
        last_flush_time (datetime): The timestamp of the last flush operation.
    """

    max_size: int
    timeout: timedelta
    flush_callable: Callable[[list[T]], Awaitable[None]]

    alive: asyncio.Event = dataclasses.field(
        init=False,
        default_factory=asyncio.Event,
    )
    events: asyncio.Queue[T] = dataclasses.field(
        init=False,
        default_factory=asyncio.Queue,
    )
    lock: asyncio.Lock = dataclasses.field(
        init=False,
        default_factory=asyncio.Lock,
    )
    last_flush_time: datetime = dataclasses.field(
        init=False,
        default_factory=helpers.perf_counter_dt,
    )

    async def add(self, item: T) -> None:
        """
        Add an item to the buffer; flush if buffer reaches maximum size or timeout.

        This method adds an item to the internal events queue. If the number of events
        in the buffer reaches or exceeds `max_size`, it triggers a flush of the buffer
        to invoke the provided asynchronous flush callable. Additionally, it checks if
        the elapsed time since the last flush has exceeded the specified timeout and
        triggers a flush if necessary.

        Args:
            item (T): The item to be added to the buffer.
        """
        async with self.lock:
            await self.events.put(item)

            if (
                self.events.qsize() >= self.max_size
                or (helpers.perf_counter_dt() - self.last_flush_time) >= self.timeout
            ):
                await self.flush()

    async def pop_until(self) -> AsyncGenerator[T, None]:
        """
        Yield items from the buffer until conditions are met.

        This asynchronous generator yields items from the internal events queue. It
        continues to yield items until either the queue is empty or the elapsed time
        since starting exceeds twice the buffer's timeout. This helps prevent the flush
        operation from taking too long or processing too many items at once.

        Yields:
            AsyncGenerator[T, None]: An asynchronous generator yielding items.
        """
        start_time = helpers.perf_counter_dt()
        for _ in range(self.max_size):
            if self.events.empty() or (helpers.perf_counter_dt() - start_time) > self.timeout:
                return
            yield await self.events.get()

    async def flush(self) -> None:
        """
        Flush the accumulated items in the buffer by invoking the provided asynchronous callable.

        Collects all items currently in the buffer by consuming the events from
        the internal queue using `pop_until`. If there are any events, it attempts to invoke the
        provided asynchronous callable with the events. If an exception occurs during the invocation
        of the callable, it logs the exception and re-adds the items to the queue for retry.
        """
        items = [item async for item in self.pop_until()]
        if not items:
            return

        try:
            await self.flush_callable(items)
        except Exception:
            logconfig.logger.exception(
                "Exception during buffer flush, waiting: %s seconds before retry.",
                self.timeout.total_seconds(),
            )
            for item in items:
                self.events.put_nowait(item)
        else:
            self.last_flush_time = helpers.perf_counter_dt()

    async def __aenter__(self) -> Self:
        """
        Enter the asynchronous context manager.

        Returns the TimedOverflowBuffer instance itself, allowing it to
        be used within an `async with` block.

        Returns:
            TimedOverflowBuffer: The TimedOverflowBuffer instance itself.
        """
        return self

    async def __aexit__(self, *_: object) -> None:
        """
        Exit the asynchronous context manager, ensuring all items are flushed.

        Flushes any remaining items in the buffer when exiting the context manager.
        """
        async with self.lock:
            while not self.events.empty():
                await self.flush()


class JobStatusLogBuffer(
    TimedOverflowBuffer[
        tuple[
            models.Job,
            models.STATUS_LOG,
        ]
    ]
):
    """
    Specialized TimedOverflowBuffer for handling Job/Status-log.
    """


class HeartbeatBuffer(TimedOverflowBuffer[models.JobId]):
    """
    Specialized TimedOverflowBuffer for handling heartbeats.
    """
