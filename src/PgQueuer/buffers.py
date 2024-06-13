from __future__ import annotations

import asyncio
import dataclasses
import time
from datetime import datetime, timedelta, timezone
from typing import Awaitable, Callable

from .models import STATUS_LOG, Job


def _perf_counter_dt() -> datetime:
    """
    Returns the current high-resolution time as a datetime object.

    This function uses the performance counter (`time.perf_counter()`) for
    the highest available resolution as a timestamp, which is useful for
    time measurements between events.
    """
    return datetime.fromtimestamp(time.perf_counter(), tz=timezone.utc)


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
        default_factory=_perf_counter_dt,
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
            self.last_event_time = _perf_counter_dt()
            if len(self.events) >= self.max_size:
                await self.flush_jobs()

    async def flush_jobs(self) -> None:
        """
        Flushes the buffer by calling the flush callback with all accumulated jobs
        and statuses. Clears the buffer after flushing.
        """
        if self.events:
            await self.flush_callback(self.events)
            self.events.clear()

    async def monitor(self) -> None:
        """
        Periodically checks if the buffer needs to be flushed based on the timeout.
        Runs until the `alive` event is cleared.
        """
        while self.alive:
            await asyncio.sleep(self.timeout.total_seconds())
            async with self.lock:
                if _perf_counter_dt() - self.last_event_time >= self.timeout:
                    await self.flush_jobs()
