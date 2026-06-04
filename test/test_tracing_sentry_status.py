"""Status and control-flow contract for ``SentryTracing.trace_process``."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Iterator

import pytest
import sentry_sdk
from pydantic_core import to_json
from sentry_sdk.transport import Transport

from pgqueuer.adapters.tracing.sentry import SentryTracing
from pgqueuer.domain.models import Job
from pgqueuer.domain.types import JobId


def _make_job(headers: dict | None) -> Job:
    now = datetime.now(timezone.utc)
    return Job(
        id=JobId(1),
        priority=0,
        created=now,
        updated=now,
        heartbeat=now,
        execute_after=now,
        status="queued",
        entrypoint="say_hello",
        payload=b"hello",
        queue_manager_id=uuid.uuid4(),
        headers=to_json(headers) if headers is not None else None,
    )


class _RecordingTransport(Transport):
    def __init__(self) -> None:
        self.transactions: list[dict[str, Any]] = []

    def capture_envelope(self, envelope: Any) -> None:
        for item in envelope.items:
            if item.type == "transaction":
                self.transactions.append(item.payload.json)


@pytest.fixture()
def transactions() -> Iterator[list[dict[str, Any]]]:
    transport = _RecordingTransport()
    previous = sentry_sdk.get_global_scope().client
    sentry_sdk.init(
        dsn="https://public@example.com/1",
        traces_sample_rate=1.0,
        transport=transport,
    )
    try:
        yield transport.transactions
    finally:
        sentry_sdk.get_global_scope().set_client(previous)


def _status(transactions: list[dict[str, Any]]) -> str:
    consumer = [t for t in transactions if t.get("transaction") == "queue_consumer_transaction"]
    assert len(consumer) == 1, [t.get("transaction") for t in transactions]
    return consumer[0]["contexts"]["trace"]["status"]


async def test_status_ok_on_success(transactions: list[dict[str, Any]]) -> None:
    tracing = SentryTracing()
    (headers,) = list(tracing.trace_publish(["say_hello"]))
    async with tracing.trace_process(_make_job(headers)):
        pass
    assert _status(transactions) == "ok"


async def test_status_internal_error_and_exception_propagates(
    transactions: list[dict[str, Any]],
) -> None:
    tracing = SentryTracing()
    (headers,) = list(tracing.trace_publish(["say_hello"]))

    with pytest.raises(ValueError, match="boom"):
        async with tracing.trace_process(_make_job(headers)):
            raise ValueError("boom")

    assert _status(transactions) == "internal_error"
