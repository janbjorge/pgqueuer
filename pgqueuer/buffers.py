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
from datetime import timedelta
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
        flush_handle (Optional[asyncio.TimerHandle]): Handle for the scheduled flush callback.
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
    flush_handle: asyncio.TimerHandle | None = dataclasses.field(
        init=False,
        default=None,
    )

    def _schedule_flush(self) -> None:
        """
        Schedule the flush_callable to be called after the specified timeout.

        If a flush is already scheduled, it cancels the previous one before scheduling a new one.
        """
        if self.flush_handle is not None:
            self.flush_handle.cancel()

        loop = asyncio.get_event_loop()
        self.flush_handle = loop.call_later(
            self.timeout.total_seconds(),
            lambda: asyncio.create_task(
                self._flush_callback(),
            ),
        )

    async def _flush_callback(self) -> None:
        """
        Callback wrapper to safely call the flush_callable coroutine.
        """
        async with self.lock:
            await self.flush()

    async def add(self, item: T) -> None:
        """
        Add an item to the buffer; flush if buffer reaches maximum size.

        This method adds an item to the internal events queue.
        If the number of events in the buffer reaches or exceeds `max_size`, it
        triggers a flush of the buffer to invoke the provided asynchronous flush callable.
        Additionally, it schedules a flush operation to occur after the specified timeout.

        Args:
            item (T): The item to be added to the buffer.
        """
        async with self.lock:
            await self.events.put(item)

            if self.events.qsize() >= self.max_size:
                await self.flush()
            else:
                self._schedule_flush()

    async def pop_until(self) -> AsyncGenerator[T, None]:
        """
        Yield items from the buffer until conditions are met.

        This asynchronous generator yields items from the internal events queue.
        It continues to yield items until either the queue is empty or the elapsed time since
        starting exceeds twice the buffer's timeout. This helps prevent the flush operation
        from taking too long or processing too many items at once.

        Yields:
            AsyncGenerator[T, None]: An asynchronous generator yielding items.
        """
        start_time = helpers.perf_counter_dt()
        for _ in range(self.max_size):
            if self.events.empty() or helpers.perf_counter_dt() - start_time > self.timeout:
                break

            yield await self.events.get()

    async def flush(self) -> None:
        """
        Flush the accumulated items in the buffer by invoking the provided asynchronous callable.

        Collects all items currently in the buffer by consuming the events from
        the internal queue using `pop_until`. If there are any events, it attempts to invoke the
        provided asynchronous callable with the events. If an exception occurs during the invocation
        of the callable, it logs the exception, re-adds the items to the queue, and schedules
        a retry. This helps in handling transient errors without losing items.
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
            # Re-add the items to the queue for retry
            for item in items:
                await self.events.put(item)
            # Schedule a retry flush
            self._schedule_flush()
            # Wait for the timeout before allowing further operations
            await asyncio.sleep(self.timeout.total_seconds())

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

        Cancels any scheduled flush operation and flushes any remaining items in the buffer.
        """
        if self.flush_handle is not None:
            self.flush_handle.cancel()

        async with self.lock:
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
