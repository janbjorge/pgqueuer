from __future__ import annotations

import asyncio
import dataclasses
from datetime import datetime, timedelta
from typing import AsyncGenerator, TypeAlias

from . import helpers, logconfig, models, queries

JobSatusTup: TypeAlias = tuple[models.Job, models.STATUS_LOG]


@dataclasses.dataclass
class JobBuffer:
    """
    A buffer class that accumulates jobs and their statuses until a specified
    capacity or timeout is reached, at which point it flushes them.

    Attributes:
        max_size (int): Maximum number of jobs the buffer can hold before
            triggering a flush.
        timeout (timedelta): Maximum time to wait before flushing the buffer,
            regardless of the buffer size.
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

    async def add_job(self, job: models.Job, status: models.STATUS_LOG) -> None:
        """
        Adds a job and its status to the buffer and flushes the buffer
        if it reaches maximum size.
        """
        await self.events.put((job, status))
        self.last_event_time = helpers.perf_counter_dt()
        if self.events.qsize() >= self.max_size:
            async with self.lock:
                if self.events.qsize() >= self.max_size:
                    await self.flush_jobs()

    async def pop_until(self) -> AsyncGenerator[JobSatusTup, None]:
        enter = helpers.perf_counter_dt()
        for _ in range(2 * self.max_size):
            if not self.events.empty() and helpers.perf_counter_dt() - enter < self.timeout * 2:
                yield await self.events.get()

    async def flush_jobs(self) -> None:
        """
        Flushes the buffer by calling the flush callback with all accumulated jobs
        and statuses. Clears the buffer after flushing.
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

    async def monitor(self) -> None:
        """
        Periodically checks if the buffer needs to be flushed based on the timeout.
        Runs until the `alive` event is cleared.
        """
        while not self.alive.is_set():
            await asyncio.sleep(self.timeout.total_seconds())
            if helpers.perf_counter_dt() - self.last_event_time >= self.timeout:
                async with self.lock:
                    if helpers.perf_counter_dt() - self.last_event_time >= self.timeout:
                        await self.flush_jobs()
