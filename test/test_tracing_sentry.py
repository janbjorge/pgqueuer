"""Behavioral coverage for the Sentry tracing adapter."""

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


def _make_job(headers: dict | None, *, entrypoint: str = "say_hello") -> Job:
    now = datetime.now(timezone.utc)
    return Job(
        id=JobId(1),
        priority=0,
        created=now,
        updated=now,
        heartbeat=now,
        execute_after=now,
        status="queued",
        entrypoint=entrypoint,
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


def _consumer(transactions: list[dict[str, Any]]) -> dict[str, Any]:
    matches = [t for t in transactions if t.get("transaction") == "queue_consumer_transaction"]
    assert len(matches) == 1, [t.get("transaction") for t in transactions]
    return matches[0]


def test_publish_injects_sentry_headers(transactions: list[dict[str, Any]]) -> None:
    (headers,) = list(SentryTracing().trace_publish(["say_hello"]))
    assert set(headers["sentry"]) == {"sentry-trace", "baggage"}
    # sentry-trace = "{trace_id}-{span_id}-{sampled}"; trace_id is 16 bytes hex.
    assert len(headers["sentry"]["sentry-trace"].split("-")[0]) == 32


def test_publish_emits_one_span_per_entrypoint(transactions: list[dict[str, Any]]) -> None:
    headers = list(SentryTracing().trace_publish(["a", "b", "c"]))
    assert len(headers) == 3
    producer = next(t for t in transactions if t.get("transaction") == "queue_producer_transaction")
    span_destinations = [s["data"]["messaging.destination.name"] for s in producer.get("spans", [])]
    assert span_destinations == ["a", "b", "c"]


async def test_consumer_continues_producer_trace(transactions: list[dict[str, Any]]) -> None:
    tracing = SentryTracing()
    (headers,) = list(tracing.trace_publish(["say_hello"]))
    publish_trace_id = headers["sentry"]["sentry-trace"].split("-")[0]

    async with tracing.trace_process(_make_job(headers)):
        pass

    assert _consumer(transactions)["contexts"]["trace"]["trace_id"] == publish_trace_id


async def test_consumer_records_job_metadata(transactions: list[dict[str, Any]]) -> None:
    tracing = SentryTracing()
    (headers,) = list(tracing.trace_publish(["say_hello"]))
    job = _make_job(headers)

    async with tracing.trace_process(job):
        pass

    (span,) = [s for s in _consumer(transactions)["spans"] if s["op"] == "queue.process"]
    assert span["data"]["messaging.message.id"] == job.id
    assert span["data"]["messaging.destination.name"] == job.entrypoint
    assert span["data"]["messaging.message.body.size"] == len(job.payload or b"")


async def test_process_is_noop_without_headers(transactions: list[dict[str, Any]]) -> None:
    async with SentryTracing().trace_process(_make_job(None)):
        pass
    assert not [t for t in transactions if t.get("transaction") == "queue_consumer_transaction"]


async def test_process_is_noop_without_sentry_key(transactions: list[dict[str, Any]]) -> None:
    async with SentryTracing().trace_process(_make_job({"otel": {"traceparent": "x"}})):
        pass
    assert not [t for t in transactions if t.get("transaction") == "queue_consumer_transaction"]
