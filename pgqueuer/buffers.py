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

from . import helpers, logconfig, models, tm

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
        callback (Callable[[list[T]], Awaitable[None]]): The asynchronous
            callable used to flush items, such as logging to a database.
        shutdown (asyncio.Event): An event to signal when the buffer should stop monitoring
            (e.g., during shutdown).
        events (asyncio.Queue[T]): An asynchronous queue holding the buffered items.
        flush_handle (Optional[asyncio.TimerHandle]): Handle for the scheduled flush callback.
    """

    max_size: int
    timeout: timedelta
    callback: Callable[[list[T]], Awaitable[None]]

    next_flush: datetime = dataclasses.field(
        init=False,
        default_factory=helpers.utc_now,
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
    delay_multiplier: float = dataclasses.field(
        init=False,
        default=1,
    )
    tm: tm.TaskManager = dataclasses.field(
        init=False,
        default_factory=tm.TaskManager,
    )

    async def periodic_flush(self) -> None:
        shutdown = asyncio.create_task(self.shutdown.wait())
        pending = set[asyncio.Task]()
        while not self.shutdown.is_set():
            if helpers.utc_now() > self.next_flush and not self.lock.locked():
                await self.flush()

            sleep_task = asyncio.create_task(
                asyncio.sleep(
                    helpers.timeout_with_jitter(
                        self.timeout,
                        self.delay_multiplier,
                    ).total_seconds()
                )
            )

            _, pending = await asyncio.wait(
                (sleep_task, shutdown),
                return_when=asyncio.FIRST_COMPLETED,
            )

        for p in pending:
            p.cancel()

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

        if (
            self.events.qsize() >= self.max_size
            and helpers.utc_now() > self.next_flush
            and not self.lock.locked()
        ):
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
        deadline = helpers.utc_now() + until
        while not self.events.empty() and helpers.utc_now() < deadline:
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

        if helpers.utc_now() < self.next_flush:
            return

        if self.lock.locked():
            return

        async with self.lock:
            items = [item async for item in self.pop_until()]

            if not items:
                return

            try:
                await self.callback(items)
            except Exception:
                self.delay_multiplier = min(256, self.delay_multiplier * 2)
                logconfig.logger.exception(
                    "Unable to flush: %s, using delay multiplier: %d",
                    self.callback.__name__,
                    self.delay_multiplier,
                )
                # Re-add the items to the queue for retry
                for item in items:
                    self.events.put_nowait(item)
            else:
                self.delay_multiplier = 1
            finally:
                self.next_flush = helpers.utc_now() + helpers.timeout_with_jitter(
                    self.timeout, self.delay_multiplier
                )

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

    async def __aexit__(self, *_: object) -> None:
        """
        Exit the asynchronous context manager, ensuring all items are flushed.

        Cancels any scheduled flush operation and flushes any remaining items in the buffer.
        """

        self.shutdown.set()
        await self.tm.gather_tasks()
        while not self.events.empty():
            await self.flush()


class JobStatusLogBuffer(TimedOverflowBuffer[tuple[models.Job, models.STATUS_LOG]]):
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
