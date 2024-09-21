"""
Module for buffering jobs and their statuses before database logging.

This module defines the `JobBuffer` class, which accumulates jobs and their statuses
until either a specified capacity is reached or a timeout occurs. It then flushes the
buffer by logging the jobs to the database. This buffering mechanism helps reduce the
number of database write operations by batching them, improving performance.
"""

from __future__ import annotations

import asyncio
import dataclasses
from datetime import datetime, timedelta
from typing import AsyncGenerator, TypeAlias

from typing_extensions import Self

from . import helpers, logconfig, models, queries

JobSatusTup: TypeAlias = tuple[models.Job, models.STATUS_LOG]


@dataclasses.dataclass
class JobBuffer:
    """
    Accumulates jobs and their statuses, flushing them to the database when conditions are met.

    The `JobBuffer` class collects jobs and their statuses in a buffer. It flushes the
    buffer when either the maximum number of jobs (`max_size`) is reached or a specified
    timeout (`timeout`) has elapsed since the last flush. The flush operation involves
    logging the jobs to the database using the provided `queries` instance.

    Attributes:
        max_size (int): The maximum number of jobs to buffer before flushing.
        timeout (timedelta): The maximum duration to wait before flushing the buffer,
            regardless of size.
        queries (queries.Queries): The `Queries` instance used to log jobs to the database.
        alive (asyncio.Event): An event to signal when the buffer should stop monitoring
            (e.g., during shutdown).
        events (asyncio.Queue[JobSatusTup]): An asynchronous queue holding the buffered jobs
            and their statuses.
        last_event_time (datetime): Timestamp of the last event added to the buffer.
        lock (asyncio.Lock): A lock to ensure thread safety during flush operations.
        flush_handle (asyncio.TimerHandle | None): Handle for the scheduled flush callback.
    """

    max_size: int
    timeout: timedelta
    queries: queries.Queries

    alive: asyncio.Event = dataclasses.field(
        init=False,
        default_factory=asyncio.Event,
    )
    events: asyncio.Queue[JobSatusTup] = dataclasses.field(
        init=False,
        default_factory=asyncio.Queue,
    )
    last_event_time: datetime = dataclasses.field(
        init=False,
        default_factory=helpers.perf_counter_dt,
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
        Schedule the flush_jobs coroutine to be called after the specified timeout.
        If a flush is already scheduled, it cancels the previous one before scheduling a new one.
        """
        if self.flush_handle is not None:
            self.flush_handle.cancel()

        loop = asyncio.get_event_loop()
        self.flush_handle = loop.call_later(
            self.timeout.total_seconds(),
            lambda: asyncio.create_task(
                self._flush_jobs_callback(),
            ),
        )

    async def _flush_jobs_callback(self) -> None:
        """
        Callback wrapper to safely call the flush_jobs coroutine.
        """
        async with self.lock:
            await self.flush_jobs()

    async def add_job(self, job: models.Job, status: models.STATUS_LOG) -> None:
        """
        Add a job and its status to the buffer; flush if buffer reaches maximum size.

        This method adds a job and its associated status to the internal events queue.
        It updates the `last_event_time` to the current time. If the number of events in
        the buffer reaches or exceeds `max_size`, it triggers a flush of the buffer to
        log the accumulated jobs to the database. Additionally, it schedules a flush
        operation to occur after the specified timeout.

        Args:
            job (models.Job): The job to be added to the buffer.
            status (models.STATUS_LOG): The status of the job
                (e.g., 'successful', 'exception', 'canceled').
        """
        async with self.lock:
            await self.events.put((job, status))
            self.last_event_time = helpers.perf_counter_dt()

            if self.events.qsize() >= self.max_size:
                await self.flush_jobs()
            else:
                self._schedule_flush()

    async def pop_until(self) -> AsyncGenerator[JobSatusTup, None]:
        """
        Yield jobs and their statuses from the buffer until conditions are met.

        This asynchronous generator yields jobs and their statuses from the internal events queue.
        It continues to yield events until either the queue is empty or the elapsed time since
        starting exceeds twice the buffer's timeout. This helps prevent the flush operation
        from taking too long or processing too many events at once.

        Yields:
            AsyncGenerator[JobSatusTup, None]: An asynchronous generator yielding
                tuples of (job, status).
        """
        enter = helpers.perf_counter_dt()
        for _ in range(2 * self.max_size):
            if not self.events.empty() and helpers.perf_counter_dt() - enter < self.timeout * 2:
                yield await self.events.get()
            else:
                break

    async def flush_jobs(self) -> None:
        """
        Flush the accumulated jobs in the buffer to the database.

        Collects all jobs and their statuses currently in the buffer by consuming the events from
        the internal queue using `pop_until`. If there are any events, it attempts to log them to
        the database using the `queries.log_jobs` method. If an exception occurs during the logging
        operation, it logs the exception and waits for the specified timeout before retrying. This
        helps in handling transient database errors without losing events.
        """
        events = [x async for x in self.pop_until()]

        if not events:
            return

        try:
            await self.queries.log_jobs(events)
        except Exception:
            logconfig.logger.exception(
                "Exception during buffer flush, waiting: %s seconds before retry.",
                self.timeout.total_seconds(),
            )
            await asyncio.sleep(self.timeout.total_seconds())
            # Optionally, you might want to re-add the events to the queue or handle retries

    async def __aenter__(self) -> Self:
        """
        Enter the asynchronous context manager.

        Returns the JobBuffer instance itself, allowing it to be used within an `async with` block.

        Returns:
            JobBuffer: The JobBuffer instance itself.
        """
        return self

    async def __aexit__(self, *_: object) -> None:
        """
        Exit the asynchronous context manager, ensuring all jobs are flushed.

        Cancels any scheduled flush operation, sets the `alive` event to signal that no
        more flushes should be scheduled, and flushes any remaining jobs in the buffer.
        """
        if self.flush_handle is not None:
            self.flush_handle.cancel()

        async with self.lock:
            await self.flush_jobs()
