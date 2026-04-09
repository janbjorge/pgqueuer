"""
Batched async buffers for heartbeats and job status logs.

Items accumulate until a size or time threshold triggers a flush
via the :meth:`~TimedOverflowBuffer.flush_items` template method.
"""

from __future__ import annotations

import asyncio
import dataclasses
import random
from contextlib import suppress
from datetime import timedelta
from typing import Generic, Protocol, TypeVar

from typing_extensions import Self

from pgqueuer.core import logconfig
from pgqueuer.domain import models

T = TypeVar("T")


@dataclasses.dataclass
class TimedOverflowBuffer(Generic[T]):
    """Batches items and flushes on size overflow or periodic timeout.

    Subclasses must override :meth:`flush_items` to process each batch.
    """

    max_size: int
    _: dataclasses.KW_ONLY
    timeout: timedelta = dataclasses.field(
        default_factory=lambda: timedelta(seconds=0.1),
    )

    events: asyncio.Queue[T] = dataclasses.field(
        init=False,
        default_factory=asyncio.Queue,
    )
    shutdown: asyncio.Event = dataclasses.field(
        init=False,
        default_factory=asyncio.Event,
    )
    lock: asyncio.Lock = dataclasses.field(
        init=False,
        default_factory=asyncio.Lock,
    )
    pending_tasks: set[asyncio.Task] = dataclasses.field(
        init=False,
        default_factory=set,
    )

    async def flush_items(self, items: list[T]) -> None:
        """Process a batch of flushed items. Subclasses must override this."""
        raise NotImplementedError

    async def add(self, item: T) -> None:
        """Add an item; trigger flush if buffer reaches *max_size*."""
        await self.events.put(item)
        if self.events.qsize() >= self.max_size and not self.lock.locked():
            self.schedule_flush()

    async def flush(self) -> None:
        """Drain the queue and call :meth:`flush_items`. Requeues on failure."""
        if self.lock.locked():
            return

        async with self.lock:
            items = self.drain_queue()

        if not items:
            return

        try:
            await self.flush_items(items)
        except Exception:
            logconfig.logger.exception("Flush failed, requeuing %d items", len(items))
            for item in items:
                self.events.put_nowait(item)

    async def __aenter__(self) -> Self:
        self.add_task(asyncio.create_task(self.periodic_flush()))
        return self

    async def __aexit__(self, *_: object) -> None:
        self.shutdown.set()
        await asyncio.gather(*list(self.pending_tasks), return_exceptions=True)

        # Best-effort drain with a few retries.
        for _attempt in range(5):
            if self.events.empty():
                break
            items = self.drain_queue()
            try:
                await self.flush_items(items)
            except Exception:
                logconfig.logger.warning("Shutdown flush failed, %d items remaining", len(items))
                for item in items:
                    self.events.put_nowait(item)
                await asyncio.sleep(0)

    def drain_queue(self) -> list[T]:
        items: list[T] = []
        while not self.events.empty():
            items.append(self.events.get_nowait())
        return items

    def schedule_flush(self) -> None:
        self.add_task(asyncio.create_task(self.flush()))

    def add_task(self, task: asyncio.Task) -> None:
        self.pending_tasks.add(task)
        task.add_done_callback(self.pending_tasks.discard)

    async def periodic_flush(self) -> None:
        while not self.shutdown.is_set():
            if not self.lock.locked() and self.events.qsize() > 0:
                await self.flush()

            with suppress(asyncio.TimeoutError, TimeoutError):
                await asyncio.wait_for(
                    self.shutdown.wait(),
                    timeout=self.timeout.total_seconds() * random.uniform(0.8, 1.2),
                )


# ---------------------------------------------------------------------------
# Narrow sink protocols (ISP)
# ---------------------------------------------------------------------------


class JobLogSink(Protocol):
    """Narrow port: accepts batched job status log entries."""

    async def log_jobs(
        self,
        job_status: list[
            tuple[
                models.Job,
                models.JOB_STATUS,
                models.TracebackRecord | None,
            ]
        ],
    ) -> None: ...


class HeartbeatSink(Protocol):
    """Narrow port: accepts batched heartbeat updates."""

    async def update_heartbeat(self, job_ids: list[models.JobId]) -> None: ...


# ---------------------------------------------------------------------------
# Concrete buffers
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class JobStatusLogBuffer(
    TimedOverflowBuffer[
        tuple[
            models.Job,
            models.JOB_STATUS,
            models.TracebackRecord | None,
        ]
    ]
):
    """Batched buffer for job completion / failure log entries."""

    repository: JobLogSink

    async def flush_items(
        self,
        items: list[
            tuple[
                models.Job,
                models.JOB_STATUS,
                models.TracebackRecord | None,
            ]
        ],
    ) -> None:
        await self.repository.log_jobs(items)


@dataclasses.dataclass
class HeartbeatBuffer(TimedOverflowBuffer[models.JobId]):
    """Batched buffer for heartbeat updates."""

    repository: HeartbeatSink

    async def flush_items(self, items: list[models.JobId]) -> None:
        await self.repository.update_heartbeat(items)
