from __future__ import annotations

import pytest

from pgqueuer.adapters.inmemory import InMemoryDriver, InMemoryQueries
from pgqueuer.adapters.tracing.opentelemetry import OpenTelemetryTracing
from pgqueuer.core.applications import PgQueuer
from pgqueuer.ports.tracing import TRACER, set_tracing_class


def test_tracing_singleton_in_ports() -> None:
    """TracingConfig and TRACER live in pgqueuer.ports.tracing."""
    from pgqueuer.ports.tracing import TRACER, TracingConfig, set_tracing_class

    assert isinstance(TRACER, TracingConfig)
    assert callable(set_tracing_class)


def test_pgqueuer_tracer_reaches_queue_manager_and_queries() -> None:
    """PgQueuer passes its tracer to the queue manager and the queries."""
    tracer = OpenTelemetryTracing()
    pgq = PgQueuer.in_memory(tracer=tracer)
    assert pgq.qm.tracer is tracer
    assert isinstance(pgq.queries, InMemoryQueries)
    assert pgq.queries.tracer is tracer


def test_pgqueuer_rejects_tracer_alongside_own_queries() -> None:
    """PgQueuer raises when its tracer is not the tracer of the given queries."""
    driver = InMemoryDriver()
    with pytest.raises(ValueError, match="same tracer as PgQueuer"):
        PgQueuer(driver, queries=InMemoryQueries(driver=driver), tracer=OpenTelemetryTracing())


def test_set_tracing_class_warns_and_sets_global(monkeypatch: pytest.MonkeyPatch) -> None:
    """set_tracing_class warns with DeprecationWarning and still sets the global tracer."""
    monkeypatch.setattr(TRACER, "tracer", None)
    tracer = OpenTelemetryTracing()
    with pytest.warns(DeprecationWarning, match=r"PgQueuer\(tracer="):
        set_tracing_class(tracer)
    assert TRACER.tracer is tracer
