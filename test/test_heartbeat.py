import asyncio
from datetime import datetime, timedelta
from typing import Awaitable, Callable

import pytest

from pgqueuer.core.buffers import HeartbeatBuffer
from pgqueuer.core.heartbeat import Heartbeat
from pgqueuer.types import JobId


class _FakeHeartbeatSink:
    """Test double satisfying the HeartbeatSink protocol."""

    def __init__(self, fn: Callable[[list[JobId]], Awaitable[None]]) -> None:
        self._fn = fn

    async def update_heartbeat(self, job_ids: list[JobId]) -> None:
        await self._fn(job_ids)


@pytest.mark.parametrize(
    "interval",
    (
        timedelta(seconds=0.01),
        timedelta(seconds=0.05),
    ),
)
async def test_heartbeat_interval(interval: timedelta) -> None:
    callbacks = list[tuple[list[JobId], datetime]]()

    async def callback(jids: list[JobId]) -> None:
        nonlocal callbacks
        callbacks.append((jids, datetime.now()))

    async with (
        HeartbeatBuffer(
            max_size=1_000,
            timeout=interval,
            repository=_FakeHeartbeatSink(callback),
        ) as buffer,
        Heartbeat(
            JobId(1),
            interval=interval / 2,
            buffer=buffer,
        ),
    ):
        await asyncio.sleep(interval.total_seconds() * 4)

    assert len(callbacks) >= 2


@pytest.mark.parametrize("max_size", (10, 100))
async def test_heartbeat_max_size(max_size: int) -> None:
    callbacks = list[tuple[list[JobId], datetime]]()

    async def callback(jids: list[JobId]) -> None:
        nonlocal callbacks
        callbacks.append((jids, datetime.now()))

    async with (
        HeartbeatBuffer(
            max_size=max_size,
            timeout=timedelta(seconds=0.01),
            repository=_FakeHeartbeatSink(callback),
        ) as buffer,
        Heartbeat(
            JobId(1),
            interval=timedelta(seconds=0.001),
            buffer=buffer,
        ),
    ):
        await asyncio.sleep(timedelta(seconds=0.1).total_seconds())

    assert len(callbacks) >= 2


async def test_heartbeat_keeps_buffer_ticking() -> None:
    class _NoopSink:
        async def update_heartbeat(self, _: list[JobId]) -> None:
            pass

    class DummyBuffer(HeartbeatBuffer):
        def __init__(self) -> None:
            super().__init__(
                max_size=10,
                timeout=timedelta(seconds=0),
                repository=_NoopSink(),
            )
            self.received = 0

        async def add(self, _: object) -> None:
            self.received += 1

    hbuf = DummyBuffer()
    async with Heartbeat(JobId(1), timedelta(seconds=0.1), hbuf):
        await asyncio.sleep(0.35)
    assert hbuf.received >= 3  # ~1 beat per 0.1 s
