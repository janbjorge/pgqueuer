# heartbeat.py

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import timedelta

from typing_extensions import Self

from . import buffers, logconfig, models


@dataclass
class Heartbeat:
    """
    Asynchronous context manager that sends heartbeats at regular intervals to a buffer.
    """

    job_id: models.JobId
    interval: timedelta
    buffer: buffers.HeartbeatBuffer
    shutdown: asyncio.Event = field(
        init=False,
        default_factory=asyncio.Event,
    )
    handle: asyncio.TimerHandle | None = field(
        init=False,
        default=None,
    )

    async def __aenter__(self) -> Self:
        """
        Enter the asynchronous context manager.

        Starts the heartbeat process by scheduling the first heartbeat.

        Returns:
            Heartbeat: The Heartbeat instance itself.
        """
        if self.interval > timedelta(seconds=0):
            self.schedule_heartbeat()
        return self

    async def __aexit__(self, *_: object) -> None:
        """
        Exit the asynchronous context manager.

        Stops the heartbeat process and flushes any remaining heartbeats in the buffer.

        Args:
            exc_type: The exception type, if any.
            exc_val: The exception value, if any.
            exc_tb: The traceback, if any.
        """
        self.shutdown.set()
        if self.handle:
            self.handle.cancel()

    def schedule_heartbeat(self) -> None:
        """
        Schedule the next heartbeat callback.
        """
        loop = asyncio.get_event_loop()
        self.handle = loop.call_later(
            self.interval.total_seconds(),
            lambda: asyncio.create_task(self.send_heartbeat()),
        )

    async def send_heartbeat(self) -> None:
        """
        Send a heartbeat by adding a JobId to the buffer and scheduling the next heartbeat.
        """
        if self.shutdown.is_set():
            return

        try:
            await self.buffer.add(self.job_id)
        except Exception as e:
            logconfig.logger.exception("Failed to send heartbeat: %s", e)
        finally:
            if not self.shutdown.is_set():
                self.schedule_heartbeat()
