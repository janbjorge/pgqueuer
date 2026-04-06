"""Backward-compatibility shim. Canonical: pgqueuer.ports.tracing"""

from pgqueuer.adapters.tracing.logfire import LogfireTracing
from pgqueuer.adapters.tracing.sentry import SentryTracing
from pgqueuer.ports.tracing import (
    TRACER,
    TracingConfig,
    TracingProtocol,
    set_tracing_class,
)

__all__ = [
    "LogfireTracing",
    "SentryTracing",
    "TracingConfig",
    "TracingProtocol",
    "TRACER",
    "set_tracing_class",
]
