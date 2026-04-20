from __future__ import annotations

import asyncio
from unittest.mock import patch

from pgqueuer import db
from pgqueuer.domain.types import ScheduleId
from pgqueuer.queries import Queries
from pgqueuer.sm import SchedulerManager


async def test_scheduler_heartbeat_batches(apgdriver: db.Driver) -> None:
    """The heartbeat loop must send batched updates for all active schedule IDs."""
    sm = SchedulerManager(apgdriver, queries=Queries(apgdriver))

    captured_calls: list[set[ScheduleId]] = []

    async def capture_heartbeat(ids: set[ScheduleId]) -> None:
        captured_calls.append(set(ids))

    # Simulate two active schedules.
    sm.active_heartbeat_ids.add(ScheduleId(1))
    sm.active_heartbeat_ids.add(ScheduleId(2))

    with patch.object(sm.queries, "update_schedule_heartbeat", side_effect=capture_heartbeat):
        # Run the heartbeat loop for ~2.5 seconds then shut down.
        async def stop_after() -> None:
            await asyncio.sleep(2.5)
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


async def test_scheduler_heartbeat_skips_when_empty(apgdriver: db.Driver) -> None:
    """The heartbeat loop must not call update when no schedules are active."""
    sm = SchedulerManager(apgdriver, queries=Queries(apgdriver))

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
