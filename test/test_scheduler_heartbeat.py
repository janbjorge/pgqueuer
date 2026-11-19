from __future__ import annotations

import asyncio
from datetime import timedelta
from unittest.mock import patch

import pytest

from pgqueuer.adapters.inmemory import InMemoryQueries
from pgqueuer.domain.models import CronExpressionEntrypoint, Schedule
from pgqueuer.domain.types import ScheduleId
from pgqueuer.sm import SchedulerManager


async def test_scheduler_heartbeat_batches(queries: InMemoryQueries) -> None:
    """The heartbeat loop must send batched updates for all active schedule IDs."""
    sm = SchedulerManager(queries)
    finished = asyncio.Event()

    captured_calls: list[set[ScheduleId]] = []

    async def capture_heartbeat(ids: set[ScheduleId]) -> None:
        captured_calls.append(set(ids))

    # Simulate two active schedules.
    sm.active_heartbeat_ids.add(ScheduleId(1))
    sm.active_heartbeat_ids.add(ScheduleId(2))

    with patch.object(sm.queries, "update_schedule_heartbeat", side_effect=capture_heartbeat):

        async def stop_after() -> None:
            await asyncio.sleep(2.5)
            sm.active_heartbeat_ids.clear()
            finished.set()

        await asyncio.gather(
            sm._heartbeat_loop(finished),
            stop_after(),
        )

    # Should have at least 1 batched call containing both IDs.
    assert captured_calls, "Expected at least one heartbeat call"
    assert any(len(c) == 2 for c in captured_calls), (
        f"Expected a call with 2 IDs, got: {captured_calls}"
    )
    assert all({ScheduleId(1), ScheduleId(2)} == c for c in captured_calls), (
        f"Unexpected IDs in calls: {captured_calls}"
    )


async def test_scheduler_heartbeat_skips_when_empty(queries: InMemoryQueries) -> None:
    """The heartbeat loop must not call update when no schedules are active."""
    sm = SchedulerManager(queries)
    finished = asyncio.Event()

    call_count = 0

    async def counting_heartbeat(ids: set[ScheduleId]) -> None:
        nonlocal call_count
        call_count += 1

    with patch.object(sm.queries, "update_schedule_heartbeat", side_effect=counting_heartbeat):

        async def stop_after() -> None:
            await asyncio.sleep(1.5)
            finished.set()

        await asyncio.gather(
            sm._heartbeat_loop(finished),
            stop_after(),
        )

    assert call_count == 0, f"Expected no heartbeat calls with empty set, got {call_count}"


async def test_scheduler_heartbeat_survives_a_failed_update(queries: InMemoryQueries) -> None:
    """One failing heartbeat update does not end the loop (#880)."""
    sm = SchedulerManager(queries)
    sm.active_heartbeat_ids.add(ScheduleId(1))
    finished = asyncio.Event()
    calls = 0

    async def flaky_heartbeat(ids: set[ScheduleId]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("transient")

    with patch.object(sm.queries, "update_schedule_heartbeat", side_effect=flaky_heartbeat):
        loop = asyncio.create_task(sm._heartbeat_loop(finished))
        await asyncio.sleep(2.5)
        assert not loop.done()
        sm.active_heartbeat_ids.clear()
        finished.set()
        await asyncio.wait_for(loop, timeout=2)

    assert calls >= 3


async def test_scheduler_heartbeat_continues_after_shutdown_while_dispatching(
    queries: InMemoryQueries,
) -> None:
    """Schedules still running at shutdown keep their heartbeat until they finish (#880)."""
    sm = SchedulerManager(queries)
    sm.active_heartbeat_ids.add(ScheduleId(1))
    finished = asyncio.Event()
    beats_after_shutdown = 0

    async def count_heartbeat(ids: set[ScheduleId]) -> None:
        nonlocal beats_after_shutdown
        if sm.shutdown.is_set():
            beats_after_shutdown += 1

    with patch.object(sm.queries, "update_schedule_heartbeat", side_effect=count_heartbeat):
        loop = asyncio.create_task(sm._heartbeat_loop(finished))
        await asyncio.sleep(0.5)
        sm.shutdown.set()
        await asyncio.sleep(2.2)
        assert not loop.done()
        sm.active_heartbeat_ids.clear()
        finished.set()
        await asyncio.wait_for(loop, timeout=2)

    assert beats_after_shutdown >= 2


async def test_scheduler_heartbeat_during_shutdown_with_pending_fetch(
    queries: InMemoryQueries,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A schedule fetched during shutdown keeps its heartbeat while draining (#1083)."""
    sm = SchedulerManager(queries)
    fetched = asyncio.Event()
    release_fetch = asyncio.Event()
    started = asyncio.Event()
    release_job = asyncio.Event()

    @sm.schedule("pending_fetch", "* * * * *", accepts_context=False)
    async def handler(schedule: Schedule) -> None:
        started.set()
        await release_job.wait()

    key = next(iter(sm.registry))
    await queries.insert_schedule({key: -timedelta(seconds=1)})
    fetch_schedule = queries.fetch_schedule

    async def delayed_fetch(
        entrypoints: dict[CronExpressionEntrypoint, timedelta],
    ) -> list[Schedule]:
        schedules = await fetch_schedule(entrypoints)
        assert len(schedules) == 1
        fetched.set()
        await release_fetch.wait()
        return schedules

    monkeypatch.setattr(queries, "fetch_schedule", delayed_fetch)
    run = asyncio.create_task(sm.run())
    try:
        await asyncio.wait_for(fetched.wait(), timeout=2)
        sm.shutdown.set()
        # Let the heartbeat observe shutdown while the fetch is still pending.
        await asyncio.sleep(0)
        release_fetch.set()
        await asyncio.wait_for(started.wait(), timeout=2)
        before = (await queries.peek_schedule())[0].heartbeat
        await asyncio.sleep(1.2)
        after = (await queries.peek_schedule())[0].heartbeat
        assert not run.done()
        assert after > before
    finally:
        release_fetch.set()
        release_job.set()
        sm.shutdown.set()
        await asyncio.wait_for(run, timeout=2)
