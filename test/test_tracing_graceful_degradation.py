"""Graceful degradation of the Sentry/Logfire tracing adapters when the SDK is absent."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import pytest
from pydantic_core import to_json

import pgqueuer.adapters.tracing.logfire as logfire_mod
import pgqueuer.adapters.tracing.sentry as sentry_mod
from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.adapters.tracing.logfire import LogfireTracing
from pgqueuer.adapters.tracing.sentry import SentryTracing
from pgqueuer.domain.models import Job
from pgqueuer.domain.types import JobId


def _make_job(headers: dict) -> Job:
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
        headers=to_json(headers),
    )


def test_sentry_publish_yields_one_header_per_entrypoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sentry_mod, "sentry_sdk", None)
    assert list(SentryTracing().trace_publish(["a", "b"])) == [{}, {}]


def test_logfire_publish_yields_one_header_per_entrypoint(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(logfire_mod, "logfire", None)
    assert list(LogfireTracing().trace_publish(["a", "b"])) == [{}, {}]


async def test_sentry_batch_enqueue_does_not_crash(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without this, merge_tracing_headers' strict zip raises on batch enqueue."""
    monkeypatch.setattr(sentry_mod, "sentry_sdk", None)
    queries = InMemoryQueries(driver=InMemoryDriver(), tracer=SentryTracing())
    ids = await queries.enqueue(["a", "b", "c"], [None, None, None], [0, 0, 0])
    assert len(ids) == 3


async def test_logfire_batch_enqueue_does_not_crash(monkeypatch: pytest.MonkeyPatch) -> None:
    """Without this, merge_tracing_headers' strict zip raises on batch enqueue."""
    monkeypatch.setattr(logfire_mod, "logfire", None)
    queries = InMemoryQueries(driver=InMemoryDriver(), tracer=LogfireTracing())
    ids = await queries.enqueue(["a", "b", "c"], [None, None, None], [0, 0, 0])
    assert len(ids) == 3


async def test_sentry_process_yields_without_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(sentry_mod, "sentry_sdk", None)
    ran = False
    async with SentryTracing().trace_process(_make_job({"sentry": {"sentry-trace": "x"}})):
        ran = True
    assert ran


async def test_logfire_process_yields_without_sdk(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(logfire_mod, "logfire", None)
    ran = False
    async with LogfireTracing().trace_process(_make_job({"logfire": {"traceparent": "x"}})):
        ran = True
    assert ran
