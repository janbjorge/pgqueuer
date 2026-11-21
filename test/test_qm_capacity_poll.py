"""A worker at capacity must wait for a NOTIFY instead of re-polling every 100 ms (#1092)."""

from __future__ import annotations

import asyncio
import contextlib
import dataclasses
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import pytest

from pgqueuer.core import listeners
from pgqueuer.db import Driver
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries

HOUR = timedelta(hours=1)
GUARD = 15


@dataclasses.dataclass
class Wait:
    """One wait the loop started: jobs picked so far, its timeout, and events already pending."""

    picked: int
    timeout: timedelta
    pending: int
    burst_heard: bool


@contextlib.asynccontextmanager
async def recorded(
    monkeypatch: pytest.MonkeyPatch,
    q: Queries,
    qm: QueueManager,
    release: asyncio.Event,
    burst_heard: asyncio.Event | None = None,
    **run_kwargs: Any,
) -> AsyncIterator[asyncio.Queue[Wait]]:
    """Run *qm* and yield every wait it starts, tagged with the jobs it has picked by then."""
    waits: asyncio.Queue[Wait] = asyncio.Queue()
    picked = 0
    dequeue = q.dequeue
    wait_for_notice_event = listeners.wait_for_notice_event

    async def counting(**kwargs: Any) -> list[Job]:
        nonlocal picked
        jobs = await dequeue(**kwargs)
        picked += len(jobs)
        return jobs

    def recording(queue: listeners.PGNoticeEventListener, timeout: timedelta) -> asyncio.Task[Any]:
        heard = burst_heard is not None and burst_heard.is_set()
        waits.put_nowait(Wait(picked, timeout, queue.qsize(), heard))
        return wait_for_notice_event(queue, timeout)

    monkeypatch.setattr(q, "dequeue", counting)
    monkeypatch.setattr(listeners, "wait_for_notice_event", recording)
    task = asyncio.create_task(qm.run(dequeue_timeout=HOUR, **run_kwargs))
    try:
        async with asyncio.timeout(GUARD):
            yield waits
    finally:
        qm.shutdown.set()
        release.set()
        await task


async def first_wait_with(waits: asyncio.Queue[Wait], picked: int) -> Wait:
    while (wait := await waits.get()).picked < picked:
        pass
    return wait


async def test_worker_budget_full_waits_full_timeout(
    apgdriver: Driver, monkeypatch: pytest.MonkeyPatch
) -> None:
    q = Queries(apgdriver)
    qm = QueueManager(q)
    release = asyncio.Event()

    @qm.entrypoint("ep")
    async def ep(job: Job) -> None:
        await release.wait()

    await q.enqueue(["ep"] * 10, [None] * 10, [0] * 10)
    async with recorded(monkeypatch, q, qm, release, batch_size=2, max_concurrent_tasks=4) as waits:
        assert (await first_wait_with(waits, 4)).timeout == HOUR


async def test_entrypoint_limit_full_waits_full_timeout(
    apgdriver: Driver, monkeypatch: pytest.MonkeyPatch
) -> None:
    q = Queries(apgdriver)
    qm = QueueManager(q)
    release = asyncio.Event()

    @qm.entrypoint("ep", concurrency_limit=1)
    async def ep(job: Job) -> None:
        await release.wait()

    await q.enqueue(["ep"] * 3, [None] * 3, [0] * 3)
    async with recorded(monkeypatch, q, qm, release) as waits:
        assert (await first_wait_with(waits, 1)).timeout == HOUR


async def test_notify_burst_wakes_saturated_worker_once(
    apgdriver: Driver, monkeypatch: pytest.MonkeyPatch
) -> None:
    """After a burst the next wait starts with nothing pending, so the burst cost one dequeue."""
    q = Queries(apgdriver)
    qm = QueueManager(q)
    release = asyncio.Event()

    @qm.entrypoint("ep")
    async def ep(job: Job) -> None:
        await release.wait()

    burst_heard = asyncio.Event()

    def on_notice(payload: str | bytes | bytearray) -> None:
        burst_heard.set()

    await q.enqueue(["ep"] * 10, [None] * 10, [0] * 10)
    async with recorded(
        monkeypatch, q, qm, release, burst_heard, batch_size=2, max_concurrent_tasks=4
    ) as waits:
        await first_wait_with(waits, 4)
        await apgdriver.add_listener(qm.channel, on_notice)
        # One statement, so all 20 distinct NOTIFYs arrive together at commit.
        await apgdriver.execute(
            """SELECT pg_notify($1::text, json_build_object(
                'channel', $1::text, 'operation', 'insert', 'table', 'q',
                'type', 'table_changed_event',
                'sent_at', NOW() + i * INTERVAL '1 microsecond')::text)
            FROM generate_series(1, 20) AS i""",
            qm.channel,
        )
        while not (wait := await waits.get()).burst_heard:
            pass
        assert wait.pending == 0, wait
