import asyncio
from collections import deque
from datetime import datetime, timezone
from typing import Dict, MutableMapping

from anyio import CancelScope

from pgqueuer.listeners import PGNoticeEventListener, handle_event_type
from pgqueuer.models import (
    AnyEvent,
    CancellationEvent,
    Context,
    EntrypointStatistics,
    JobId,
    RequestsPerSecondEvent,
    TableChangedEvent,
)


async def test_handle_table_changed_event() -> None:
    notice_event_queue = PGNoticeEventListener()
    statistics: Dict[str, EntrypointStatistics] = {
        "entrypoint_1": EntrypointStatistics(
            samples=deque(), concurrency_limiter=asyncio.Semaphore(5)
        )
    }
    canceled: MutableMapping[JobId, Context] = {}

    event = AnyEvent(
        root=TableChangedEvent(
            channel="channel_1",
            sent_at=datetime.now(tz=timezone.utc),
            type="table_changed_event",
            operation="insert",
            table="jobs_table",
        )
    )

    handle_event_type(event, notice_event_queue, statistics, canceled)

    assert not notice_event_queue.empty()
    assert notice_event_queue.qsize() == 1
    assert isinstance(notice_event_queue.get_nowait(), TableChangedEvent)


async def test_handle_requests_per_second_event() -> None:
    notice_event_queue = PGNoticeEventListener()
    statistics = {
        "entrypoint_1": EntrypointStatistics(
            samples=deque(),
            concurrency_limiter=asyncio.Semaphore(5),
        )
    }
    canceled: MutableMapping[JobId, Context] = {}

    event = AnyEvent(
        root=RequestsPerSecondEvent(
            channel="channel_1",
            sent_at=datetime.now(tz=timezone.utc),
            type="requests_per_second_event",
            entrypoint_count={"entrypoint_1": 10},
        )
    )

    handle_event_type(event, notice_event_queue, statistics, canceled)

    assert len(statistics["entrypoint_1"].samples) == 1
    assert statistics["entrypoint_1"].samples[0] == (10, event.root.sent_at)


async def test_handle_cancellation_event() -> None:
    notice_event_queue = PGNoticeEventListener()
    statistics = {
        "entrypoint_1": EntrypointStatistics(
            samples=deque(),
            concurrency_limiter=asyncio.Semaphore(5),
        )
    }
    canceled: MutableMapping[JobId, Context] = {}
    cancellation_context = Context(cancellation=CancelScope())
    job_id = JobId(123)
    canceled[job_id] = cancellation_context

    event = AnyEvent(
        root=CancellationEvent(
            channel="channel_1",
            sent_at=datetime.now(tz=timezone.utc),
            type="cancellation_event",
            ids=[job_id],
        )
    )

    handle_event_type(event, notice_event_queue, statistics, canceled)

    assert cancellation_context.cancellation.cancel_called
