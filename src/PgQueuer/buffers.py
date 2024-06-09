from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable

from .models import STATUS_LOG, Job


class JobBuffer:
    def __init__(
        self,
        max_size: int,
        timeout: float,
        flush_callback: Callable[[list[tuple[Job, STATUS_LOG]]], Awaitable[None]],
    ):
        self.max_size = max_size
        self.timeout = timeout
        self.flush_callback = flush_callback

        self.alive = asyncio.Event()
        self.alive.set()

        self.events = list[tuple[Job, STATUS_LOG]]()
        self.last_event_time = time.perf_counter()
        self.lock = asyncio.Lock()

    async def add_job(self, job: Job, status: STATUS_LOG) -> None:
        async with self.lock:
            self.events.append((job, status))
            self.last_event_time = time.perf_counter()
            if len(self.events) >= self.max_size:
                await self.flush_jobs()

    async def flush_jobs(self) -> None:
        if self.events:
            await self.flush_callback(self.events)
            self.events.clear()

    async def monitor(self) -> None:
        while self.alive.is_set():
            await asyncio.sleep(self.timeout)
            async with self.lock:
                if time.perf_counter() - self.last_event_time >= self.timeout:
                    await self.flush_jobs()
