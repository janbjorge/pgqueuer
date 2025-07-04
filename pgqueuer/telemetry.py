"""OpenTelemetry helpers for optional tracing integration."""

from __future__ import annotations

from contextlib import contextmanager
from types import ModuleType
from typing import Iterator


def _try_load_otel() -> tuple[ModuleType | None, ModuleType | None, ModuleType | None]:
    """Attempt to import OpenTelemetry modules.

    Returns a tuple containing the ``trace``, ``propagate``, and ``context`` modules
    if they are available, otherwise ``None`` for each missing component.
    """
    try:
        from opentelemetry import (
            context as otel_context,
            propagate,
            trace,
        )
    except Exception:  # pragma: no cover - import failure path
        return None, None, None

    return trace, propagate, otel_context


_trace, _propagate, _context = _try_load_otel()


def capture_headers() -> dict[str, str] | None:
    """Capture the current trace context as a dictionary.

    If OpenTelemetry is not installed, or no context is active, ``None`` is
    returned. The headers are returned as a dictionary suitable for storage in
    the ``JSONB`` column of the queue table.
    """
    if _trace is None or _propagate is None:
        return None

    carrier: dict[str, str] = {}
    _propagate.inject(carrier)
    return carrier or None


@contextmanager
def span_from_headers(name: str, headers: dict[str, str] | None) -> Iterator[None]:
    """Start a tracing span using the provided headers.

    The context from ``headers`` is extracted before the span starts and is
    detached when the span ends. If OpenTelemetry is unavailable, this yields a
    no-op context manager.
    """
    if _trace is None or _propagate is None or _context is None:
        yield
        return

    token = None
    if headers:
        try:
            ctx = _propagate.extract(headers)
            token = _context.attach(ctx)
        except Exception:  # pragma: no cover - malformed headers
            token = None

    tracer = _trace.get_tracer("pgqueuer")
    with tracer.start_as_current_span(name):
        try:
            yield
        finally:
            if token is not None:
                _context.detach(token)
