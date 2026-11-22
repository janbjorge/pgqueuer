from __future__ import annotations

import asyncio
import logging
from collections.abc import Iterator
from datetime import timedelta

import pytest

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.core import logconfig
from pgqueuer.domain.errors import FailingListenerError
from pgqueuer.domain.models import HealthCheckEvent, utc_now
from pgqueuer.domain.types import HealthCheckId
from pgqueuer.qm import QueueManager


class ErrorRecords(logging.Handler):
    """Collect records at ERROR or above and signal the first one."""

    def __init__(self) -> None:
        super().__init__(level=logging.ERROR)
        self.records: list[logging.LogRecord] = []
        self.logged = asyncio.Event()

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)
        self.logged.set()


@pytest.fixture
def error_records() -> Iterator[ErrorRecords]:
    handler = ErrorRecords()
    logconfig.logger.addHandler(handler)
    yield handler
    logconfig.logger.removeHandler(handler)


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


async def test_failed_periodic_health_check_is_logged(
    monkeypatch: pytest.MonkeyPatch,
    error_records: ErrorRecords,
) -> None:
    """A failed periodic probe is logged once while the run goes on (#1025)."""
    qm = QueueManager(InMemoryQueries(InMemoryDriver()))

    async def listener_healthy(timeout: timedelta) -> HealthCheckEvent:
        raise FailingListenerError

    monkeypatch.setattr(qm, "listener_healthy", listener_healthy)

    run = asyncio.create_task(qm.run())
    try:
        async with asyncio.timeout(5):
            await error_records.logged.wait()
        assert not run.done()
    finally:
        qm.shutdown.set()
        await run

    (record,) = error_records.records
    assert record.levelno == logging.ERROR
    assert "listener health check failed" in record.getMessage().lower()
    assert record.exc_info is not None
    assert record.exc_info[0] is FailingListenerError
