from __future__ import annotations

import asyncio
from unittest.mock import patch

from pgqueuer.adapters.inmemory import InMemoryQueries
from pgqueuer.domain.types import ScheduleId
from pgqueuer.sm import SchedulerManager


async def test_scheduler_heartbeat_batches(queries: InMemoryQueries) -> None:
    """The heartbeat loop must send batched updates for all active schedule IDs."""
    sm = SchedulerManager(queries)

    captured_calls: list[set[ScheduleId]] = []

    async def capture_heartbeat(ids: set[ScheduleId]) -> None:
        captured_calls.append(set(ids))

    # Simulate two active schedules.
    sm.active_heartbeat_ids.add(ScheduleId(1))
    sm.active_heartbeat_ids.add(ScheduleId(2))

    with patch.object(sm.queries, "update_schedule_heartbeat", side_effect=capture_heartbeat):
        # Run the heartbeat loop for ~2.5 seconds, finish the schedules, then shut down.
        async def stop_after() -> None:
            await asyncio.sleep(2.5)
            sm.active_heartbeat_ids.clear()
            sm.shutdown.set()

        await asyncio.gather(
            sm._heartbeat_loop(),
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

    call_count = 0

    async def counting_heartbeat(ids: set[ScheduleId]) -> None:
        nonlocal call_count
        call_count += 1

    with patch.object(sm.queries, "update_schedule_heartbeat", side_effect=counting_heartbeat):

        async def stop_after() -> None:
            await asyncio.sleep(1.5)
            sm.shutdown.set()

        await asyncio.gather(
            sm._heartbeat_loop(),
            stop_after(),
        )

    assert call_count == 0, f"Expected no heartbeat calls with empty set, got {call_count}"


async def test_scheduler_heartbeat_survives_a_failed_update(queries: InMemoryQueries) -> None:
    """One failing heartbeat update does not end the loop (#880)."""
    sm = SchedulerManager(queries)
    sm.active_heartbeat_ids.add(ScheduleId(1))
    calls = 0

    async def flaky_heartbeat(ids: set[ScheduleId]) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ConnectionError("transient")

    with patch.object(sm.queries, "update_schedule_heartbeat", side_effect=flaky_heartbeat):
        loop = asyncio.create_task(sm._heartbeat_loop())
        await asyncio.sleep(2.5)
        assert not loop.done()
        sm.active_heartbeat_ids.clear()
        sm.shutdown.set()
        await asyncio.wait_for(loop, timeout=2)

    assert calls >= 3


async def test_scheduler_heartbeat_continues_after_shutdown_while_dispatching(
    queries: InMemoryQueries,
) -> None:
    """Schedules still running at shutdown keep their heartbeat until they finish (#880)."""
    sm = SchedulerManager(queries)
    sm.active_heartbeat_ids.add(ScheduleId(1))
    beats_after_shutdown = 0

    async def count_heartbeat(ids: set[ScheduleId]) -> None:
        nonlocal beats_after_shutdown
        if sm.shutdown.is_set():
            beats_after_shutdown += 1

    with patch.object(sm.queries, "update_schedule_heartbeat", side_effect=count_heartbeat):
        loop = asyncio.create_task(sm._heartbeat_loop())
        await asyncio.sleep(0.5)
        sm.shutdown.set()
        await asyncio.sleep(2.2)
        assert not loop.done()
        sm.active_heartbeat_ids.clear()
        await asyncio.wait_for(loop, timeout=2)

    assert beats_after_shutdown >= 2
