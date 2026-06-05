"""Port protocol for tracing operations.

This module defines the tracing contract that core code depends on.
The existing ``LogfireTracing`` and ``SentryTracing`` classes satisfy
this protocol via structural subtyping.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import AsyncContextManager, Final, Generator, Protocol

from pgqueuer.models import Job


class TracingProtocol(Protocol):
    """Tracing operations for queue producer and consumer paths."""

    def trace_publish(self, entrypoints: list[str]) -> Generator[dict, None, None]:
        """Yield one tracing-header dict per entrypoint, in input order."""
        ...

    def trace_process(self, job: Job) -> AsyncContextManager[None]:
        """Wrap consumer processing of *job* in a tracing span."""
        ...


@dataclass
class TracingConfig:
    """Holds the active tracer, or ``None`` when tracing is disabled."""

    tracer: TracingProtocol | None = None


TRACER: Final[TracingConfig] = TracingConfig()


def set_tracing_class(tracer: TracingProtocol) -> None:
    """Install *tracer* as the global tracing implementation."""
    TRACER.tracer = tracer
