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
import time
from contextlib import suppress
from datetime import timedelta
from typing import (
    AsyncGenerator,
    Awaitable,
    Callable,
    Generic,
    TypeVar,
)

import backoff
from typing_extensions import Self

from . import helpers, logconfig, models, tm
from .errors import FlushException

T = TypeVar("T")


@dataclasses.dataclass
class TimedOverflowBuffer(Generic[T]):
    """
    Accumulates items, flushing them based on timeouts or buffer capacity.

    The `TimedOverflowBuffer` class collects items in a buffer and flushes them
    when either the maximum number of items (`max_size`) is reached or a specified
    timeout (`timeout`) has elapsed since the last flush. Flushing involves invoking
    an asynchronous callback with the accumulated items.

    The class includes mechanisms for retrying failed flush operations and ensures
    a graceful shutdown by attempting to flush remaining items before exit.

    Attributes:
        max_size (int): The maximum number of items to buffer before triggering a flush.
        timeout (timedelta): The maximum duration to wait before automatically flushing
            the buffer, regardless of size.
        callback (Callable[[list[T]], Awaitable[None]]): The asynchronous callable invoked
            during a flush operation to process the buffered items.
        shutdown (asyncio.Event): An event that signals when the buffer should stop
            operations, such as during shutdown.
        events (asyncio.Queue[T]): An asynchronous queue holding the buffered items.
        lock (asyncio.Lock): A lock to prevent concurrent flush operations.
        tm (tm.TaskManager): A task manager for managing background tasks associated
            with the buffer's operations.
    """

    max_size: int
    callback: Callable[[list[T]], Awaitable[None]]
    timeout: timedelta = dataclasses.field(
        default_factory=lambda: timedelta(seconds=0.1),
    )
    shutdown: asyncio.Event = dataclasses.field(
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
    tm: tm.TaskManager = dataclasses.field(
        init=False,
        default_factory=tm.TaskManager,
    )

    async def periodic_flush(self) -> None:
        while not self.shutdown.is_set():
            if not self.lock.locked() and self.events.qsize() > 0:
                self.tm.add(asyncio.create_task(self.flush()))

            # await asyncio.sleep(helpers.timeout_with_jitter(self.timeout).total_seconds())
            with suppress(asyncio.TimeoutError, TimeoutError):
                await asyncio.wait_for(
                    asyncio.create_task(self.shutdown.wait()),
                    timeout=helpers.timeout_with_jitter(self.timeout).total_seconds(),
                )

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
        await self.events.put(item)

        if self.events.qsize() >= self.max_size and not self.lock.locked():
            self.tm.add(asyncio.create_task(self.flush()))

    async def pop_until(
        self,
        until: timedelta = timedelta(seconds=0.01),
    ) -> AsyncGenerator[T, None]:
        """
        Yield items from the buffer until conditions are met.

        This asynchronous generator yields items from the internal events queue.
        It continues to yield items until either the queue is empty or the elapsed time since
        starting exceeds twice the buffer's timeout. This helps prevent the flush operation
        from taking too long or processing too many items at once.

        Yields:
            AsyncGenerator[T, None]: An asynchronous generator yielding items.
        """
        deadline = time.time() + until.total_seconds()
        while not self.events.empty() and time.time() < deadline:
            yield await self.events.get()

    async def _flush_once(self) -> None:
        """
        Flush the accumulated items in the buffer by invoking the provided asynchronous callable.

        Collects all items currently in the buffer by consuming the events from
        the internal queue using `pop_until`. If there are any events, it attempts to invoke the
        provided asynchronous callable with the events. If an exception occurs during the invocation
        of the callable, it logs the exception, re-adds the items to the queue, and schedules
        a retry. This helps in handling transient errors without losing items.

        Raises:
        FlushException: When the callback fails, allowing the backoff decorator
            on flush() to handle retries.

        """

        if self.lock.locked():
            return

        async with self.lock:
            items = [item async for item in self.pop_until()]

            if not items:
                return

            try:
                await self.callback(items)
            except Exception as e:
                logconfig.logger.warning(
                    "Unable to flush(%s): %s\n",
                    self.callback.__name__,
                    str(e),
                )

                # Re-add the items to the queue for retry
                for item in items:
                    self.events.put_nowait(item)

                raise FlushException(f"Error during flush operation: {e}") from e

    @backoff.on_exception(
        backoff.expo,
        FlushException,
        max_time=10,
        on_backoff=lambda details: logconfig.logger.warning(
            "Unable to flush: Retry in %0.2f seconds after  %d tries",
            details["wait"],
            details["tries"],
        ),
    )
    async def flush(self) -> None:
        """
        Flush the accumulated items in the buffer with automatic retry on failure.

        Uses exponential backoff to retry failed flush operations for up to 10 seconds.
        The actual flush logic is handled by _flush_once(), while this method provides
        the retry wrapper using the litl/backoff library.

        Note:
            It is intentional that backoff is only applied to the FlushException.

        """
        await self._flush_once()

    async def __aenter__(self) -> Self:
        """
        Enter the asynchronous context manager.

        Returns the TimedOverflowBuffer instance itself, allowing it to
        be used within an `async with` block.

        Returns:
            TimedOverflowBuffer: The TimedOverflowBuffer instance itself.
        """
        self.tm.add(asyncio.create_task(self.periodic_flush()))
        return self

    @backoff.on_exception(
        backoff.expo,
        FlushException,
        max_time=5,
        raise_on_giveup=False,
    )
    async def _shutdown_flush(self) -> None:
        """
        Perform final flush attempt during shutdown with limited retry.

        Uses exponential backoff from litl/backoff for up to 5 seconds
        to handle transient failures during shutdown.

        Note:
        - We only attempt flush if items are present
        - Items are kept are leftover if they fail to flush
        """
        if not self.events.empty():
            await self._flush_once()

    async def __aexit__(self, *_: object) -> None:
        """
        Exit the asynchronous context manager, ensuring all items are flushed.

        This method is called when exiting an `async with` block. It ensures the buffer
        performs any final flush operations for the accumulated items before shutdown.
        The following steps are executed:

        1. Signals the buffer to stop periodic operations by setting the `shutdown` event.
        2. Waits for all ongoing tasks managed by the buffer to complete.
        3. Attempts to flush any remaining items in the buffer using the `_shutdown_flush` method
         to handle transient failures, such as temporary database outages.

        Note:
            This mechanism ensures that the application attempts to process all items
            gracefully before shutting down. However, if the flush operation repeatedly
            fails (e.g., due to a database outage), the method will eventually give up
            to prevent blocking the application indefinitely.
        """

        self.shutdown.set()
        await self.tm.gather_tasks()
        await self._shutdown_flush()


class JobStatusLogBuffer(
    TimedOverflowBuffer[
        tuple[
            models.Job,
            models.JOB_STATUS,
            models.TracebackRecord | None,
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


class RequestsPerSecondBuffer(TimedOverflowBuffer[str]):
    """
    Specialized TimedOverflowBuffer for handling RPS.
    """
