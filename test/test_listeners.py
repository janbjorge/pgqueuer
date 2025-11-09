import asyncio
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import MutableMapping

import pytest
from anyio import CancelScope
from async_timeout import timeout

from pgqueuer import db
from pgqueuer.listeners import (
    EventRouter,
    PGNoticeEventListener,
    default_event_router,
    initialize_notice_event_listener,
)
from pgqueuer.models import (
    AnyEvent,
    CancellationEvent,
    Channel,
    Context,
    EntrypointStatistics,
    HealthCheckEvent,
    JobId,
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
    pending_health_check: MutableMapping[uuid.UUID, asyncio.Future[HealthCheckEvent]] = {}

    event = AnyEvent(
        root=TableChangedEvent(
            channel="channel_1",
            sent_at=datetime.now(tz=timezone.utc),
            type="table_changed_event",
            operation="insert",
            table="jobs_table",
        )
    )
    default_event_router(
        notice_event_queue=notice_event_queue,
        statistics=statistics,
        canceled=canceled,
        pending_health_check=pending_health_check,
    )(event)

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
    pending_health_check: MutableMapping[uuid.UUID, asyncio.Future[HealthCheckEvent]] = {}

    event = AnyEvent(
        root=RequestsPerSecondEvent(
            channel="channel_1",
            sent_at=datetime.now(tz=timezone.utc),
            type="requests_per_second_event",
            entrypoint_count={"entrypoint_1": 10},
        )
    )

    default_event_router(
        notice_event_queue=notice_event_queue,
        statistics=statistics,
        canceled=canceled,
        pending_health_check=pending_health_check,
    )(event)

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
    pending_health_check: MutableMapping[uuid.UUID, asyncio.Future[HealthCheckEvent]] = {}

    cancellation_context = Context(
        cancellation=CancelScope(), resources={"test_key": "listener_test"}
    )
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

    default_event_router(
        notice_event_queue=notice_event_queue,
        statistics=statistics,
        canceled=canceled,
        pending_health_check=pending_health_check,
    )(event)

    assert cancellation_context.cancellation.cancel_called


async def test_handle_health_check_event_event() -> None:
    notice_event_queue = PGNoticeEventListener()
    statistics = {
        "entrypoint_1": EntrypointStatistics(
            samples=deque(),
            concurrency_limiter=asyncio.Semaphore(5),
        )
    }
    canceled: MutableMapping[JobId, Context] = {}
    pending_health_check: MutableMapping[uuid.UUID, asyncio.Future[HealthCheckEvent]] = {}

    event = AnyEvent(
        root=HealthCheckEvent(
            channel="channel_1",
            sent_at=datetime.now(timezone.utc),
            type="health_check_event",
            id=uuid.uuid4(),
        )
    )
    assert isinstance(event.root, HealthCheckEvent)
    pending_health_check[event.root.id] = asyncio.Future()

    default_event_router(
        notice_event_queue=notice_event_queue,
        statistics=statistics,
        canceled=canceled,
        pending_health_check=pending_health_check,
    )(event)

    assert pending_health_check[event.root.id].result().id == event.root.id


async def test_emit_stable_changed_insert(apgdriver: db.Driver) -> None:
    evnets = list[AnyEvent]()
    await initialize_notice_event_listener(
        apgdriver,
        Channel(DBSettings().channel),
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
        Channel(DBSettings().channel),
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
        global_concurrency_limit=1000,
    )
    async with timeout(1):
        while len(evnets) < 1:
            await asyncio.sleep(0)

    (update_event,) = evnets
    assert update_event.root.type == "table_changed_event"
    assert update_event.root.table == add_prefix("pgqueuer")
    assert update_event.root.operation == "update"
    evnets.clear()


async def test_emits_truncate_table_truncate(apgdriver: db.Driver) -> None:
    evnets = list[AnyEvent]()
    await initialize_notice_event_listener(
        apgdriver,
        Channel(DBSettings().channel),
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
        Channel(DBSettings().channel),
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


async def test_trigger_notifies_on_insert(apgdriver: db.Driver) -> None:
    """Test that INSERT operations trigger notifications."""
    evnets = list[AnyEvent]()
    await initialize_notice_event_listener(
        apgdriver,
        Channel(DBSettings().channel),
        evnets.append,
    )

    await Queries(apgdriver).enqueue("test_insert", None)

    async with timeout(1):
        while len(evnets) < 1:
            await asyncio.sleep(0)

    assert len(evnets) == 1
    assert evnets[0].root.type == "table_changed_event"
    assert evnets[0].root.operation == "insert"


async def test_trigger_notifies_on_delete(apgdriver: db.Driver) -> None:
    """Test that DELETE operations trigger notifications."""
    evnets = list[AnyEvent]()
    queries = Queries(apgdriver)

    (job_id,) = await queries.enqueue("test_delete", None)
    await asyncio.sleep(0.1)

    await initialize_notice_event_listener(
        apgdriver,
        Channel(DBSettings().channel),
        evnets.append,
    )

    await queries.driver.execute(
        f"DELETE FROM {queries.qbe.settings.queue_table} WHERE id = $1", job_id
    )

    async with timeout(1):
        while len(evnets) < 1:
            await asyncio.sleep(0)

    assert len(evnets) == 1
    assert evnets[0].root.type == "table_changed_event"
    assert evnets[0].root.operation == "delete"


async def test_trigger_notifies_on_meaningful_update(apgdriver: db.Driver) -> None:
    """Test that meaningful UPDATE operations (non-heartbeat) trigger notifications."""
    evnets = list[AnyEvent]()
    queries = Queries(apgdriver)

    (job_id,) = await queries.enqueue("test_update", None)
    await asyncio.sleep(0.1)

    await initialize_notice_event_listener(
        apgdriver,
        Channel(DBSettings().channel),
        evnets.append,
    )

    # Update a meaningful column (status)
    await queries.driver.execute(
        f"UPDATE {queries.qbe.settings.queue_table} SET status = 'picked' WHERE id = $1",
        job_id,
    )

    async with timeout(1):
        while len(evnets) < 1:
            await asyncio.sleep(0)

    assert len(evnets) == 1
    assert evnets[0].root.type == "table_changed_event"
    assert evnets[0].root.operation == "update"


async def test_trigger_ignores_multiple_heartbeat_updates(apgdriver: db.Driver) -> None:
    """Test that multiple consecutive heartbeat updates don't trigger notifications."""
    evnets = list[AnyEvent]()
    queries = Queries(apgdriver)

    (job_id,) = await queries.enqueue("test_multi_heartbeat", None)

    async with timeout(1):
        while len(evnets) < 1:
            await asyncio.sleep(0)

    await initialize_notice_event_listener(
        apgdriver,
        Channel(DBSettings().channel),
        evnets.append,
    )
    evnets.clear()

    # Perform 20 heartbeat updates
    for _ in range(20):
        await queries.update_heartbeat([job_id])
        await asyncio.sleep(0.01)

    await asyncio.sleep(0.2)
    assert len(evnets) == 0


async def test_trigger_notifies_on_update_after_heartbeat(apgdriver: db.Driver) -> None:
    """Test that meaningful updates are notified even after heartbeat updates."""
    evnets = list[AnyEvent]()
    queries = Queries(apgdriver)

    (job_id,) = await queries.enqueue("test_mixed", None)
    await asyncio.sleep(0.1)

    await initialize_notice_event_listener(
        apgdriver,
        Channel(DBSettings().channel),
        evnets.append,
    )
    evnets.clear()

    # Do some heartbeat updates (should not trigger notifications)
    await queries.update_heartbeat([job_id])
    await queries.update_heartbeat([job_id])
    await asyncio.sleep(0.1)
    assert len(evnets) == 0

    # Now do a meaningful update (should trigger notification)
    await queries.driver.execute(
        f"UPDATE {queries.qbe.settings.queue_table} SET priority = 10 WHERE id = $1",
        job_id,
    )

    async with timeout(1):
        while len(evnets) < 1:
            await asyncio.sleep(0)

    assert len(evnets) == 1
    assert evnets[0].root.type == "table_changed_event"
    assert evnets[0].root.operation == "update"


async def test_trigger_notifies_on_truncate(apgdriver: db.Driver) -> None:
    """Test that TRUNCATE operations trigger notifications."""
    evnets = list[AnyEvent]()
    queries = Queries(apgdriver)

    await queries.enqueue("test_truncate", None)
    await asyncio.sleep(0.1)

    await initialize_notice_event_listener(
        apgdriver,
        Channel(DBSettings().channel),
        evnets.append,
    )
    evnets.clear()

    await queries.clear_queue()

    async with timeout(1):
        while len(evnets) < 1:
            await asyncio.sleep(0)

    assert len(evnets) == 1
    assert evnets[0].root.type == "table_changed_event"
    assert evnets[0].root.operation == "truncate"


def test_event_router_dispatches_correct_handler() -> None:
    router = EventRouter()
    called = {"flag": False}

    @router.register("table_changed_event")
    def _handle(evt: TableChangedEvent) -> None:
        called["flag"] = evt.table == "jobs_table"

    event = AnyEvent(
        root=TableChangedEvent(
            channel="chan",
            sent_at=datetime.now(tz=timezone.utc),
            type="table_changed_event",
            operation="insert",
            table="jobs_table",
        )
    )

    router(event)
    assert called["flag"] is True


def test_event_router_duplicate_registration_raises() -> None:
    router = EventRouter()

    @router.register("table_changed_event")
    def _handler(evt: TableChangedEvent) -> None:
        pass

    with pytest.raises(ValueError):

        @router.register("table_changed_event")
        def _another(evt: TableChangedEvent) -> None:
            pass


def test_event_router_missing_handler_raises() -> None:
    router = EventRouter()

    event = AnyEvent(
        root=TableChangedEvent(
            channel="chan",
            sent_at=datetime.now(tz=timezone.utc),
            type="table_changed_event",
            operation="insert",
            table="jobs_table",
        )
    )

    with pytest.raises(NotImplementedError):
        router(event)
