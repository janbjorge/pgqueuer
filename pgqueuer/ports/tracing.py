"""Port protocol for tracing operations."""

from __future__ import annotations

import warnings
from collections.abc import Generator
from contextlib import AbstractAsyncContextManager
from dataclasses import dataclass
from typing import Final, Protocol, runtime_checkable

from pgqueuer.domain.models import Job


class TracingProtocol(Protocol):
    """Tracing operations for queue producer and consumer paths."""

    def trace_publish(self, entrypoints: list[str]) -> Generator[dict[str, object], None, None]:
        """Yield one tracing-header dict per entrypoint, in input order."""
        ...

    def trace_process(self, job: Job) -> AbstractAsyncContextManager[None]:
        """Wrap consumer processing of *job* in a tracing span."""
        ...


@runtime_checkable
class TracedRepository(Protocol):
    """Repository that carries its own tracer."""

    tracer: TracingProtocol | None


@dataclass
class TracingConfig:
    """Holds the active tracer, or ``None`` when tracing is disabled."""

    tracer: TracingProtocol | None = None


TRACER: Final[TracingConfig] = TracingConfig()


def set_tracing_class(tracer: TracingProtocol) -> None:
    """Install *tracer* as the global tracing implementation.

    Deprecated: pass ``tracer=`` to ``PgQueuer``, or to your own ``Queries``.
    Removed in the next major release.
    """
    warnings.warn(
        "set_tracing_class() is deprecated; pass PgQueuer(tracer=...), or tracer= to your Queries.",
        DeprecationWarning,
        stacklevel=2,
    )
    TRACER.tracer = tracer
