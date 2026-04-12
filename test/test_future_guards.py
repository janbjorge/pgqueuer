from __future__ import annotations

import asyncio
import uuid
from datetime import datetime, timezone
from typing import MutableMapping

from pgqueuer import db
from pgqueuer.core.completion import CompletionWatcher
from pgqueuer.core.listeners import (
    PGNoticeEventListener,
    default_event_router,
)
from pgqueuer.models import (
    AnyEvent,
    Context,
    HealthCheckEvent,
    Job,
    JobId,
)
from pgqueuer.qm import QueueManager
from pgqueuer.types import QueueExecutionMode


async def test_health_check_callback_ignores_done_future() -> None:
    """Router callback must not crash when the future is already resolved."""
    notice_event_queue = PGNoticeEventListener()
    canceled: MutableMapping[JobId, Context] = {}
    pending_health_check: MutableMapping[uuid.UUID, asyncio.Future[HealthCheckEvent]] = {}

    event = AnyEvent(
        root=HealthCheckEvent(
            channel="ch",
            sent_at=datetime.now(timezone.utc),
            type="health_check_event",
            id=uuid.uuid4(),
        )
    )
    assert isinstance(event.root, HealthCheckEvent)

    # Pre-resolve the future before the callback fires.
    fut: asyncio.Future[HealthCheckEvent] = asyncio.Future()
    fut.set_result(event.root)
    pending_health_check[event.root.id] = fut

    # Must not raise InvalidStateError.
    default_event_router(
        notice_event_queue=notice_event_queue,
        canceled=canceled,
        pending_health_check=pending_health_check,
    )(event)


async def test_health_check_callback_ignores_cancelled_future() -> None:
    """Router callback must not crash when the future is already cancelled."""
    notice_event_queue = PGNoticeEventListener()
    canceled: MutableMapping[JobId, Context] = {}
    pending_health_check: MutableMapping[uuid.UUID, asyncio.Future[HealthCheckEvent]] = {}

    event = AnyEvent(
        root=HealthCheckEvent(
            channel="ch",
            sent_at=datetime.now(timezone.utc),
            type="health_check_event",
            id=uuid.uuid4(),
        )
    )
    assert isinstance(event.root, HealthCheckEvent)

    fut: asyncio.Future[HealthCheckEvent] = asyncio.Future()
    fut.cancel()
    pending_health_check[event.root.id] = fut

    # Must not raise InvalidStateError.
    default_event_router(
        notice_event_queue=notice_event_queue,
        canceled=canceled,
        pending_health_check=pending_health_check,
    )(event)


async def test_completion_waiter_ignores_cancelled_future(apgdriver: db.Driver) -> None:
    """_refresh_waiters must not crash when a waiter future is already cancelled."""
    qm = QueueManager(apgdriver)

    @qm.entrypoint("task")
    async def task(job: Job) -> None: ...

    jids = await qm.queries.enqueue(["task"], [None], [0])
    await qm.run(mode=QueueExecutionMode.drain)

    async with CompletionWatcher(apgdriver) as watcher:
        waiter = watcher.wait_for(jids[0])
        # Cancel the waiter before refresh resolves it.
        waiter.cancel()
        # Force a refresh — must not raise InvalidStateError.
        await watcher._refresh_waiters()
