from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from typing import Final, Generator, Protocol

try:
    import logfire
    import logfire.propagate
except ImportError:
    logfire = None  # type: ignore[assignment]


from pgqueuer.models import Job


@dataclass
class TracingConfig:
    """
    Configuration object for tracing setup.

    Attributes:
        enabled (bool): Whether tracing is enabled globally.
    """

    tracer: TracingProtocol | None = None


TRACER: Final[TracingConfig] = TracingConfig()


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

    @contextmanager
    def trace_process(self, job: Job) -> Generator[None, None, None]:
        """
        Async context manager for tracing queue consumer job processing.

        Args:
            job (Job): The job being processed, containing headers and metadata.

        Yields:
            None: This context manager does not return a value but manages the tracing lifecycle.
        """
        ...


class LogfireTracing(TracingProtocol):
    def trace_publish(self, entrypoints: list[str]) -> Generator[dict, None, None]:
        if logfire is None:
            yield {}
            return

        with logfire.span("Enqueue"):
            for entrypoint in entrypoints:
                with logfire.span("Enqueued: {entrypoint=}", entrypoint=entrypoint):
                    yield {"logfire": dict(logfire.propagate.get_context())}

    @contextmanager
    def trace_process(self, job: Job) -> Generator[None, None, None]:
        """
        Async context manager for tracing queue consumer job processing.

        Args:
            job (Job): The job being processed, containing headers and metadata.

        Yields:
            None: This context manager does not return a value but manages the tracing lifecycle.
        """

        if logfire is None:
            yield
            return

        if job.headers is None:
            yield
            return

        log_fire_headers: dict | None = job.headers.get("logfire")

        if log_fire_headers is None:
            yield
            return

        with (
            logfire.propagate.attach_context(log_fire_headers),
            logfire.span(
                "Processing job: {job_id=}",
                job_id=job.id,
                entrypoint=job.entrypoint,
                payload_size=len(job.payload or b""),
            ),
        ):
            yield


def set_tracing_class(tracer: TracingProtocol) -> None:
    """
    Sets the tracing instance for the PGQueuer system.

    Args:
        tracer (TracingProtocol): An instance implementing the TracingProtocol.
    """

    TRACER.tracer = tracer
