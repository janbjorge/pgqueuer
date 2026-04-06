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
    """
    Protocol defining the interface for tracing operations in the PGQueuer system.

    This protocol ensures that any tracing implementation provides methods for:
    - Publishing tracing headers for queue producer operations.
    - Managing tracing for queue consumer job processing.
    """

    def trace_publish(self, entrypoints: list[str]) -> Generator[dict, None, None]:
        """
        Publishes tracing headers for queue producer operations.

        Args:
            entrypoints (list[str]): A list of entrypoints representing queue destinations.

        Yields:
            dict: A dictionary containing tracing headers for each entrypoint.
        """
        ...

    def trace_process(self, job: Job) -> AsyncContextManager[None]:
        """
        Async context manager for tracing queue consumer job processing.

        Args:
            job (Job): The job being processed, containing headers and metadata.

        Yields:
            None: This context manager does not return a value but manages the tracing lifecycle.
        """
        ...


@dataclass
class TracingConfig:
    """
    Configuration object for tracing setup.

    Attributes:
        tracer: The active tracing implementation, or ``None`` when tracing is disabled.
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
