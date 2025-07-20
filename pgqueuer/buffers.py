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
from typing import AsyncGenerator, Awaitable, Callable, Generic, TypeVar

import tenacity
from typing_extensions import Self

from . import helpers, logconfig, models, tm

T = TypeVar("T")


@dataclasses.dataclass(slots=True)
class RetrySettings:
    """Configuration for exponential backoff retries."""

    start_delay: float = 0.01
    base: float = 2.0
    max_delay: float = 10.0
    max_time: float = float("inf")


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
        retry_settings (RetrySettings): Configuration for retrying failed flush operations.
        shutdown_settings (RetrySettings): Retry configuration used during shutdown
            to ensure all items are flushed before the process exits.
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
    retry_settings: RetrySettings = dataclasses.field(
        default_factory=RetrySettings,
    )
    shutdown_settings: RetrySettings = dataclasses.field(
        default_factory=lambda: RetrySettings(
            start_delay=0.001,
            max_delay=0.1,
            max_time=0.1,
        ),
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

    async def _flush_with_settings(self, settings: RetrySettings) -> None:
        """Flush accumulated items with exponential backoff retries."""

        if self.lock.locked():
            return

        async with self.lock:
            items = [item async for item in self.pop_until()]

        if not items:
            return

        def _log(retry_state: tenacity.RetryCallState) -> None:
            if retry_state.outcome is None or retry_state.next_action is None:
                return
            if retry_state.outcome.failed:
                exc = retry_state.outcome.exception()
                logconfig.logger.warning(
                    "Unable to flush(%s): %s\nRetry in: %.2fs",
                    self.callback.__name__,
                    str(exc),
                    retry_state.next_action.sleep,
                )

        try:
            async for attempt in tenacity.AsyncRetrying(
                wait=tenacity.wait.wait_exponential(
                    multiplier=settings.start_delay,
                    min=settings.start_delay,
                    max=settings.max_delay,
                    exp_base=settings.base,
                ),
                stop=(
                    tenacity.stop.stop_after_delay(settings.max_time)
                    if settings.max_time != float("inf")
                    else tenacity.stop.stop_never
                ),
                reraise=True,
                before_sleep=_log,
            ):
                with attempt:
                    await self.callback(items)
        except Exception:  # pragma: no cover - rare failure path
            for item in items:
                self.events.put_nowait(item)
        else:
            return

    async def flush(self) -> None:
        await self._flush_with_settings(self.retry_settings)

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

        This method is called when exiting an `async with` block. It ensures the buffer
        performs any final flush operations for the accumulated items before shutdown.
        The following steps are executed:

        1. Signals the buffer to stop periodic operations by setting the `shutdown` event.
        2. Waits for all ongoing tasks managed by the buffer to complete.
        3. Attempts to flush any remaining items in the buffer using the `shutdown_settings`
           retry strategy to handle transient failures, such as temporary database outages.
        4. Stops retrying after the maximum time defined in `shutdown_settings`
           is reached. This prevents the application from hanging indefinitely
           during situations like prolonged database downtime or critical errors.

        Note:
            This mechanism ensures that the application attempts to process all items
            gracefully before shutting down. However, if the flush operation repeatedly
            fails (e.g., due to a database outage), the method will eventually give up
            to prevent blocking the application indefinitely.
        """

        self.shutdown.set()
        await self.tm.gather_tasks()

        await self._flush_with_settings(self.shutdown_settings)


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
