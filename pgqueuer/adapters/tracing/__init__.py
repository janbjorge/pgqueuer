from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from pgqueuer.ports.tracing import TracingProtocol


@dataclass
class TracingConfig:
    """
    Configuration object for tracing setup.

    Attributes:
        enabled (bool): Whether tracing is enabled globally.
    """

    tracer: TracingProtocol | None = None


TRACER: Final[TracingConfig] = TracingConfig()


def set_tracing_class(tracer: TracingProtocol) -> None:
    """
    Sets the tracing instance for the PGQueuer system.

    Args:
        tracer (TracingProtocol): An instance implementing the TracingProtocol.
    """

    TRACER.tracer = tracer
