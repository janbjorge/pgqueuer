import asyncio
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import MutableMapping

from anyio import CancelScope
from async_timeout import timeout

from pgqueuer import db
from pgqueuer.listeners import (
    PGNoticeEventListener,
    handle_event_type,
    initialize_notice_event_listener,
)
from pgqueuer.models import (
    AnyEvent,
    CancellationEvent,
    Context,
    EntrypointStatistics,
    JobId,
    PGChannel,
    RequestsPerSecondEvent,
    TableChangedEvent,
)
from pgqueuer.qb import DBSettings, add_prefix
from pgqueuer.queries import EntrypointExecutionParameter, Queries


async def test_handle_table_changed_event() -> None:
    notice_event_queue = PGNoticeEventListener()
    statistics = {
        "entrypoint_1": EntrypointStatistics(
            samples=deque(),
            concurrency_limiter=asyncio.Semaphore(5),
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


async def test_emit_stable_changed_insert(apgdriver: db.Driver) -> None:
    evnets = list[AnyEvent]()
    await initialize_notice_event_listener(
        apgdriver,
        PGChannel(DBSettings().channel),
        evnets.append,
    )

    (job_id,) = await Queries(apgdriver).enqueue(
        "test_emit_stable_changed_insert",
        None,
    )

    async with timeout(1):
        while len(evnets) < 1:
            await asyncio.sleep(0)

    (event,) = evnets

    assert event.root.type == "table_changed_event"
    assert event.root.table == add_prefix("pgqueuer")
    assert event.root.operation == "insert"


async def test_emit_stable_changed_update(apgdriver: db.Driver) -> None:
    evnets = list[AnyEvent]()
    await initialize_notice_event_listener(
        apgdriver,
        PGChannel(DBSettings().channel),
        evnets.append,
    )

    await Queries(apgdriver).enqueue(
        "test_emit_stable_changed_update",
        None,
    )

    async with timeout(1):
        while len(evnets) < 1:
            await asyncio.sleep(0)

    (event,) = evnets

    assert event.root.type == "table_changed_event"
    assert event.root.table == add_prefix("pgqueuer")
    assert event.root.operation == "insert"
    evnets.clear()

    await Queries(apgdriver).dequeue(
        100,
        {
            "test_emit_stable_changed_update": EntrypointExecutionParameter(
                timedelta(seconds=300), False, 0
            )
        },
        uuid.uuid4(),
    )
    await asyncio.sleep(0.1)
    assert len(evnets) == 0


async def test_emits_truncate_table_truncate(apgdriver: db.Driver) -> None:
    evnets = list[AnyEvent]()
    await initialize_notice_event_listener(
        apgdriver,
        PGChannel(DBSettings().channel),
        evnets.append,
    )

    await Queries(apgdriver).enqueue(
        "test_emits_truncate_table_truncate",
        None,
    )
    async with timeout(1):
        while len(evnets) < 1:
            await asyncio.sleep(0)

    evnets.clear()

    await Queries(apgdriver).clear_queue()
    async with timeout(1):
        while len(evnets) < 1:
            await asyncio.sleep(0)

    (event,) = evnets

    assert event.root.type == "table_changed_event"
    assert event.root.table == add_prefix("pgqueuer")
    assert event.root.operation == "truncate"
    evnets.clear()


async def test_pgqueuer_heartbeat_event_trigger(apgdriver: db.Driver) -> None:
    evnets = list[AnyEvent]()
    await initialize_notice_event_listener(
        apgdriver,
        PGChannel(DBSettings().channel),
        evnets.append,
    )

    (job_id,) = await Queries(apgdriver).enqueue(
        "test_pgqueuer_heartbeat_event_trigger",
        None,
    )

    async with timeout(1):
        while len(evnets) < 1:
            await asyncio.sleep(0)

    (event,) = evnets
    assert event.root.type == "table_changed_event"
    assert event.root.table == add_prefix("pgqueuer")
    evnets.clear()

    await asyncio.gather(
        *[Queries(apgdriver).update_heartbeat([job_id]) for _ in range(10)],
    )
    await asyncio.sleep(0.1)
    assert len(evnets) == 0
