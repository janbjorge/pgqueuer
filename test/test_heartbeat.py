import asyncio
from datetime import datetime, timedelta

import pytest

from pgqueuer.buffers import HeartbeatBuffer
from pgqueuer.heartbeat import Heartbeat
from pgqueuer.types import JobId


@pytest.mark.parametrize("interval", (timedelta(seconds=0.1), timedelta(seconds=0.01)))
async def test_heartbeat_interval(interval: timedelta) -> None:
    callbacks = list[tuple[list[JobId], datetime]]()

    async def callback(jids: list[JobId]) -> None:
        nonlocal callbacks
        callbacks.append((jids, datetime.now()))

    async with (
        HeartbeatBuffer(
            max_size=1_000,
            timeout=interval,
            callback=callback,
        ) as buffer,
        Heartbeat(
            JobId(1),
            interval=interval / 2,
            buffer=buffer,
        ),
    ):
        await asyncio.sleep(interval.total_seconds() * 2)

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
            callback=callback,
        ) as buffer,
        Heartbeat(
            JobId(1),
            interval=timedelta(seconds=0.001),
            buffer=buffer,
        ),
    ):
        await asyncio.sleep(timedelta(seconds=0.1).total_seconds())

    assert len(callbacks) >= 2
