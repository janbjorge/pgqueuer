# heartbeat.py

from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import timedelta

from typing_extensions import Self

from pgqueuer.core import buffers, logconfig
from pgqueuer.domain import models


@dataclass
class Heartbeat:
    """Async context manager that sends periodic heartbeats to a buffer."""

    job_id: models.JobId
    interval: timedelta
    buffer: buffers.HeartbeatBuffer
    shutdown: asyncio.Event = field(
        init=False,
        default_factory=asyncio.Event,
    )
    heartbeat_task: asyncio.Task[None] | None = field(
        init=False,
        default=None,
    )

    async def __aenter__(self) -> Self:
        if self.interval > timedelta(seconds=0):
            self.heartbeat_task = asyncio.create_task(self.send_heartbeat())
        return self

    async def __aexit__(self, *_: object) -> None:
        self.shutdown.set()
        if self.heartbeat_task is not None:
            with suppress(asyncio.CancelledError):
                await self.heartbeat_task

    async def send_heartbeat(self) -> None:
        while not self.shutdown.is_set():
            try:
                await self.buffer.add(self.job_id)
            except Exception as e:
                logconfig.logger.exception("Failed to send heartbeat: %s", e)
            finally:
                with suppress(TimeoutError, asyncio.TimeoutError):
                    await asyncio.wait_for(
                        self.shutdown.wait(),
                        timeout=self.interval.total_seconds(),
                    )
