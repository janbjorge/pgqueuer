from __future__ import annotations

import asyncio
import dataclasses
from datetime import datetime, timedelta
from typing import Awaitable, Callable

from .helpers import perf_counter_dt
from .logconfig import logger
from .models import STATUS_LOG, Job


@dataclasses.dataclass
class JobBuffer:
    """
    A buffer class that accumulates jobs and their statuses until a specified
    capacity or timeout is reached, at which point it flushes them using a
    provided callback function.

    Attributes:
        max_size (int): Maximum number of jobs the buffer can hold before
            triggering a flush.
        timeout (timedelta): Maximum time to wait before flushing the buffer,
            regardless of the buffer size.
        flush_callback (Callable[[list[tuple[Job, STATUS_LOG]]], Awaitable[None]]):
            Asynchronous callback function to process jobs when the buffer is flushed.
    """

    max_size: int
    timeout: timedelta
    flush_callback: Callable[
        [list[tuple[Job, STATUS_LOG]]],
        Awaitable[None],
    ]

    alive: bool = dataclasses.field(
        init=False,
        default=True,
    )
    events: list[tuple[Job, STATUS_LOG]] = dataclasses.field(
        init=False,
        default_factory=list,
    )
    last_event_time: datetime = dataclasses.field(
        init=False,
        default_factory=perf_counter_dt,
    )
    lock: asyncio.Lock = dataclasses.field(
        init=False,
        default_factory=asyncio.Lock,
    )

    async def add_job(self, job: Job, status: STATUS_LOG) -> None:
        """
        Adds a job and its status to the buffer and flushes the buffer
        if it reaches maximum size.
        """
        async with self.lock:
            self.events.append((job, status))
            self.last_event_time = perf_counter_dt()
            if len(self.events) >= self.max_size:
                await self.flush_jobs()

    async def flush_jobs(self) -> None:
        """
        Flushes the buffer by calling the flush callback with all accumulated jobs
        and statuses. Clears the buffer after flushing.
        """
        while self.events:
            try:
                await self.flush_callback(self.events)
            except Exception:
                logger.exception(
                    "Exception during buffer flush, waiting: %s seconds before retry.",
                    self.timeout.total_seconds(),
                )
                await asyncio.sleep(self.timeout.total_seconds())
            else:
                self.events.clear()

    async def monitor(self) -> None:
        """
        Periodically checks if the buffer needs to be flushed based on the timeout.
        Runs until the `alive` event is cleared.
        """
        while self.alive:
            await asyncio.sleep(self.timeout.total_seconds())
            async with self.lock:
                if perf_counter_dt() - self.last_event_time >= self.timeout:
                    await self.flush_jobs()
