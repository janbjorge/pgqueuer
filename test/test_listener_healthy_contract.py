from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.domain.errors import FailingListenerError
from pgqueuer.domain.models import HealthCheckEvent, utc_now
from pgqueuer.domain.types import HealthCheckId
from pgqueuer.qm import QueueManager


async def test_listener_healthy_raises_on_timeout(monkeypatch: pytest.MonkeyPatch) -> None:
    """listener_healthy must raise FailingListenerError directly on timeout."""
    queries = InMemoryQueries(InMemoryDriver())
    qm = QueueManager(queries)

    async def notify_health_check(health_check_event_id: HealthCheckId) -> None:
        pass

    monkeypatch.setattr(queries, "notify_health_check", notify_health_check)

    with pytest.raises(FailingListenerError):
        await qm.listener_healthy(timeout=timedelta(milliseconds=100))

    assert qm.pending_health_check == {}


async def test_listener_healthy_propagates_cancel_racing_with_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A reply arriving with cancellation must not make the probe swallow cancellation."""
    queries = InMemoryQueries(InMemoryDriver())
    qm = QueueManager(queries)

    async def notify_health_check(health_check_event_id: HealthCheckId) -> None:
        future = qm.pending_health_check[health_check_event_id]
        task = asyncio.current_task()
        assert task is not None

        def reply_and_cancel() -> None:
            future.set_result(
                HealthCheckEvent(
                    channel=qm.channel,
                    sent_at=utc_now(),
                    type="health_check_event",
                    id=health_check_event_id,
                )
            )
            task.cancel()

        asyncio.get_running_loop().call_soon(reply_and_cancel)

    monkeypatch.setattr(queries, "notify_health_check", notify_health_check)

    with pytest.raises(asyncio.CancelledError):
        await qm.listener_healthy(timeout=timedelta(seconds=1))

    assert qm.pending_health_check == {}
