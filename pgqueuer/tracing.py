"""Backward-compatibility shim. Canonical: pgqueuer.adapters.tracing + pgqueuer.ports.tracing"""

from pgqueuer.adapters.tracing import (
    TRACER,
    TracingConfig,
    set_tracing_class,
)
from pgqueuer.adapters.tracing.logfire import LogfireTracing
from pgqueuer.adapters.tracing.sentry import SentryTracing
from pgqueuer.ports.tracing import TracingProtocol

__all__ = [
    "LogfireTracing",
    "SentryTracing",
    "TracingConfig",
    "TracingProtocol",
    "TRACER",
    "set_tracing_class",
]
