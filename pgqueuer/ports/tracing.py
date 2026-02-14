"""Port protocol for tracing operations.

This module defines the tracing contract that core code depends on.
The existing ``LogfireTracing`` and ``SentryTracing`` classes satisfy
this protocol via structural subtyping.
"""

from __future__ import annotations

from typing import AsyncContextManager, Generator, Protocol

from ..models import Job


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
