"""Behavioral coverage for the Logfire tracing adapter."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, Iterator

import logfire
import pytest
from logfire.testing import TestExporter
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from pydantic_core import to_json

from pgqueuer.adapters.tracing.logfire import LogfireTracing
from pgqueuer.domain.models import Job
from pgqueuer.domain.types import JobId


def _make_job(headers: dict | None, *, entrypoint: str = "say_hello") -> Job:
    now = datetime.now(timezone.utc)
    return Job(
        id=JobId(7),
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


@pytest.fixture()
def exporter() -> Iterator[TestExporter]:
    exp = TestExporter()
    logfire.configure(
        send_to_logfire=False,
        additional_span_processors=[SimpleSpanProcessor(exp)],
    )
    yield exp


def _spans(exporter: TestExporter) -> list[dict[str, Any]]:
    return exporter.exported_spans_as_dict()


def _by_name(exporter: TestExporter, needle: str) -> dict[str, Any]:
    matches = [s for s in _spans(exporter) if needle in s["name"]]
    assert len(matches) == 1, [s["name"] for s in _spans(exporter)]
    return matches[0]


def test_publish_injects_traceparent(exporter: TestExporter) -> None:
    (headers,) = list(LogfireTracing().trace_publish(["say_hello"]))
    assert "traceparent" in headers["logfire"]


def test_publish_emits_span_per_entrypoint(exporter: TestExporter) -> None:
    headers = list(LogfireTracing().trace_publish(["a", "b"]))
    assert len(headers) == 2
    enqueued = [s for s in _spans(exporter) if "Enqueued" in s["name"]]
    assert len(enqueued) == 2


async def test_consumer_continues_producer_trace(exporter: TestExporter) -> None:
    tracing = LogfireTracing()
    (headers,) = list(tracing.trace_publish(["say_hello"]))
    publish_trace_ids = {s["context"]["trace_id"] for s in _spans(exporter)}
    assert len(publish_trace_ids) == 1

    async with tracing.trace_process(_make_job(headers)):
        pass

    processing = _by_name(exporter, "Processing job")
    assert processing["context"]["trace_id"] in publish_trace_ids


async def test_consumer_records_job_metadata(exporter: TestExporter) -> None:
    tracing = LogfireTracing()
    (headers,) = list(tracing.trace_publish(["say_hello"]))
    job = _make_job(headers)

    async with tracing.trace_process(job):
        pass

    attrs = _by_name(exporter, "Processing job")["attributes"]
    assert attrs["job_id"] == job.id
    assert attrs["entrypoint"] == job.entrypoint
    assert attrs["payload_size"] == len(job.payload or b"")


async def test_process_is_noop_without_headers(exporter: TestExporter) -> None:
    async with LogfireTracing().trace_process(_make_job(None)):
        pass
    assert not [s for s in _spans(exporter) if "Processing job" in s["name"]]


async def test_process_is_noop_without_logfire_key(exporter: TestExporter) -> None:
    async with LogfireTracing().trace_process(_make_job({"otel": {"traceparent": "x"}})):
        pass
    assert not [s for s in _spans(exporter) if "Processing job" in s["name"]]
