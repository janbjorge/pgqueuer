"""Unit tests for OpenTelemetryTracing adapter.

Uses the OpenTelemetry SDK's InMemorySpanExporter so no external
collector is needed.  The tests verify that:
- Span names, kinds, and attributes follow OTel messaging semconv.
- W3C TraceContext headers are injected into job headers on publish.
- Single-message and batch-message paths produce the correct span shapes.
- The consumer span is a child of the producer creation span (same trace_id).
- A failing job handler marks the span with ERROR status.
- The adapter degrades gracefully when opentelemetry is not installed.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace import SpanKind, StatusCode
from pydantic_core import to_json

from pgqueuer.adapters.tracing.opentelemetry import OpenTelemetryTracing
from pgqueuer.domain.models import Job
from pgqueuer.domain.types import JobId

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_job(headers: dict | None = None, entrypoint: str = "say_hello") -> Job:
    """Create a minimal Job for testing.

    ``Job.headers`` has a ``BeforeValidator`` that expects JSON-encoded
    bytes/str, so we serialise the dict before passing it in — matching
    what ``InMemoryQueries.enqueue`` does via ``pydantic_core.to_json``.
    """
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


@pytest.fixture()
def exporter() -> InMemorySpanExporter:
    return InMemorySpanExporter()


@pytest.fixture()
def tracer_provider(exporter: InMemorySpanExporter) -> TracerProvider:
    provider = TracerProvider()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return provider


@pytest.fixture()
def otel_tracer(tracer_provider: TracerProvider) -> trace.Tracer:
    return tracer_provider.get_tracer("test")


@pytest.fixture()
def tracing(otel_tracer: trace.Tracer) -> OpenTelemetryTracing:
    return OpenTelemetryTracing(tracer=otel_tracer)


# ---------------------------------------------------------------------------
# trace_publish — single message
# Spec: single send → one PRODUCER span named "send {destination}"
# ---------------------------------------------------------------------------


def test_single_publish_yields_otel_header(
    tracing: OpenTelemetryTracing,
) -> None:
    (headers,) = tracing.trace_publish(["fetch"])
    assert "otel" in headers
    assert "traceparent" in headers["otel"]


def test_single_publish_span_name(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    list(tracing.trace_publish(["fetch"]))
    names = [s.name for s in exporter.get_finished_spans()]
    assert names == ["send fetch"]


def test_single_publish_span_kind_is_producer(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    list(tracing.trace_publish(["fetch"]))
    (span,) = exporter.get_finished_spans()
    assert span.kind == SpanKind.PRODUCER


def test_single_publish_attributes(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    list(tracing.trace_publish(["fetch"]))
    (span,) = exporter.get_finished_spans()
    assert span.attributes is not None
    assert span.attributes["messaging.destination.name"] == "fetch"
    assert span.attributes["messaging.system"] == "pgqueuer"
    assert span.attributes["messaging.operation.name"] == "send"
    assert span.attributes["messaging.operation.type"] == "send"


# ---------------------------------------------------------------------------
# trace_publish — batch (multiple messages)
# Spec: batch → CLIENT "send" parent + PRODUCER "create {destination}" children
# ---------------------------------------------------------------------------


def test_batch_publish_yields_otel_headers_for_each(
    tracing: OpenTelemetryTracing,
) -> None:
    headers = list(tracing.trace_publish(["a", "b", "c"]))
    assert len(headers) == 3
    for h in headers:
        assert "otel" in h
        assert "traceparent" in h["otel"]


def test_batch_publish_span_names(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    list(tracing.trace_publish(["a", "b"]))
    names = {s.name for s in exporter.get_finished_spans()}
    assert "send" in names
    assert "create a" in names
    assert "create b" in names


def test_batch_publish_send_span_kind_is_client(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    list(tracing.trace_publish(["a", "b"]))
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert spans["send"].kind == SpanKind.CLIENT


def test_batch_publish_create_span_kind_is_producer(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    list(tracing.trace_publish(["a", "b"]))
    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert spans["create a"].kind == SpanKind.PRODUCER
    assert spans["create b"].kind == SpanKind.PRODUCER


def test_batch_publish_send_span_attributes(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    list(tracing.trace_publish(["a", "b", "c"]))
    spans = {s.name: s for s in exporter.get_finished_spans()}
    attrs = spans["send"].attributes
    assert attrs is not None
    assert attrs["messaging.system"] == "pgqueuer"
    assert attrs["messaging.operation.name"] == "send"
    assert attrs["messaging.operation.type"] == "send"
    assert attrs["messaging.batch.message_count"] == 3


def test_batch_publish_create_span_attributes(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    list(tracing.trace_publish(["fetch"]))
    # single → "send fetch", not "create fetch"
    list(tracing.trace_publish(["a", "b"]))
    spans = {s.name: s for s in exporter.get_finished_spans()}
    attrs = spans["create a"].attributes
    assert attrs is not None
    assert attrs["messaging.destination.name"] == "a"
    assert attrs["messaging.system"] == "pgqueuer"
    assert attrs["messaging.operation.name"] == "create"
    assert attrs["messaging.operation.type"] == "create"


# ---------------------------------------------------------------------------
# trace_process
# ---------------------------------------------------------------------------


async def test_trace_process_creates_consumer_span(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    (headers,) = tracing.trace_publish(["say_hello"])
    job = _make_job(headers=headers)

    async with tracing.trace_process(job):
        pass

    names = {s.name for s in exporter.get_finished_spans()}
    assert "process say_hello" in names


async def test_trace_process_span_kind_is_consumer(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    (headers,) = tracing.trace_publish(["say_hello"])
    job = _make_job(headers=headers)

    async with tracing.trace_process(job):
        pass

    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert spans["process say_hello"].kind == SpanKind.CONSUMER


async def test_trace_process_span_name_uses_entrypoint(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    (headers,) = tracing.trace_publish(["send_email"])
    job = _make_job(headers=headers, entrypoint="send_email")

    async with tracing.trace_process(job):
        pass

    names = {s.name for s in exporter.get_finished_spans()}
    assert "process send_email" in names


async def test_trace_process_linked_to_producer(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    """Consumer span must share trace_id with the producer span."""
    (headers,) = tracing.trace_publish(["say_hello"])
    job = _make_job(headers=headers)

    async with tracing.trace_process(job):
        pass

    finished = exporter.get_finished_spans()
    producer = next(s for s in finished if s.name == "send say_hello")
    consumer = next(s for s in finished if s.name == "process say_hello")

    assert producer.context is not None
    assert consumer.context is not None
    assert producer.context.trace_id == consumer.context.trace_id


async def test_trace_process_attributes(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    (headers,) = tracing.trace_publish(["say_hello"])
    job = _make_job(headers=headers)

    async with tracing.trace_process(job):
        pass

    spans = {s.name: s for s in exporter.get_finished_spans()}
    attrs = spans["process say_hello"].attributes
    assert attrs is not None
    assert attrs["messaging.destination.name"] == "say_hello"
    assert attrs["messaging.system"] == "pgqueuer"
    assert attrs["messaging.operation.name"] == "process"
    assert attrs["messaging.operation.type"] == "process"
    assert attrs["messaging.message.id"] == str(job.id)
    assert attrs["messaging.message.body.size"] == len(job.payload or b"")


async def test_trace_process_ok_status_on_success(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    (headers,) = tracing.trace_publish(["say_hello"])
    job = _make_job(headers=headers)

    async with tracing.trace_process(job):
        pass

    spans = {s.name: s for s in exporter.get_finished_spans()}
    assert spans["process say_hello"].status.status_code == StatusCode.OK


async def test_trace_process_error_status_on_exception(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    (headers,) = tracing.trace_publish(["say_hello"])
    job = _make_job(headers=headers)

    with pytest.raises(ValueError, match="boom"):
        async with tracing.trace_process(job):
            raise ValueError("boom")

    spans = {s.name: s for s in exporter.get_finished_spans()}
    span = spans["process say_hello"]
    assert span.status.status_code == StatusCode.ERROR
    assert span.status.description is not None
    assert "boom" in span.status.description
    assert span.attributes is not None
    assert span.attributes["error.type"] == "ValueError"


async def test_trace_process_no_headers_is_noop(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    job = _make_job(headers=None)
    async with tracing.trace_process(job):
        pass

    assert not any(s.name.startswith("process") for s in exporter.get_finished_spans())


async def test_trace_process_missing_otel_key_is_noop(
    tracing: OpenTelemetryTracing,
    exporter: InMemorySpanExporter,
) -> None:
    job = _make_job(headers={"logfire": {"some": "context"}})
    async with tracing.trace_process(job):
        pass

    assert not any(s.name.startswith("process") for s in exporter.get_finished_spans())


# ---------------------------------------------------------------------------
# Graceful degradation when opentelemetry is not installed
# ---------------------------------------------------------------------------


def test_trace_publish_graceful_without_otel(monkeypatch: pytest.MonkeyPatch) -> None:
    import pgqueuer.adapters.tracing.opentelemetry as otel_mod

    monkeypatch.setattr(otel_mod, "opentelemetry", None)
    t = OpenTelemetryTracing()
    headers = list(t.trace_publish(["ep_a", "ep_b"]))
    assert headers == [{}, {}]


async def test_trace_process_graceful_without_otel(monkeypatch: pytest.MonkeyPatch) -> None:
    import pgqueuer.adapters.tracing.opentelemetry as otel_mod

    monkeypatch.setattr(otel_mod, "opentelemetry", None)
    t = OpenTelemetryTracing()
    job = _make_job(headers={"otel": {"traceparent": "00-abc-def-01"}})

    reached = False
    async with t.trace_process(job):
        reached = True
    assert reached


# ---------------------------------------------------------------------------
# Job.otel_headers helper
# ---------------------------------------------------------------------------


def test_job_otel_headers_present() -> None:
    carrier = {"traceparent": "00-abc123-def456-01"}
    job = _make_job(headers={"otel": carrier})
    assert job.otel_headers() == carrier


def test_job_otel_headers_absent() -> None:
    job = _make_job(headers={"logfire": {}})
    assert job.otel_headers() is None


def test_job_otel_headers_no_headers() -> None:
    job = _make_job(headers=None)
    assert job.otel_headers() is None
