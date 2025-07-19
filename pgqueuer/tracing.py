from __future__ import annotations

from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncContextManager, AsyncIterator, Final, Generator, Protocol

try:
    import logfire
    import logfire.propagate
except ImportError:
    logfire = None  # type: ignore[assignment]

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None  # type: ignore[assignment]

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

    def trace_process(self, job: Job) -> AsyncContextManager[None]:
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

    @asynccontextmanager
    async def trace_process(self, job: Job) -> AsyncIterator[None]:
        """
        Synchronous context manager for tracing queue consumer job processing.

        Args:
            job (Job): The job being processed, containing headers and metadata.

        Yields:
            None: This context manager does not return a value but manages the tracing lifecycle.
        """

        if logfire is None or job.headers is None:
            yield
            return

        if not (log_fire_headers := job.logfire_headers()):
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


class SentryTracing(TracingProtocol):
    def trace_publish(self, entrypoints: list[str]) -> Generator[dict, None, None]:
        if sentry_sdk is None:
            yield {}
            return

        with sentry_sdk.start_transaction(
            op="function",
            name="queue_producer_transaction",
        ) as transaction:
            for entrypoint in entrypoints:
                with sentry_sdk.start_span(
                    op="queue.publish",
                    name="queue_producer",
                ) as span:
                    span.set_data("messaging.destination.name", entrypoint)
                    yield {
                        "sentry": {
                            "sentry-trace": sentry_sdk.get_traceparent(),
                            "baggage": sentry_sdk.get_baggage(),
                        }
                    }
            transaction.set_status("ok")

    @asynccontextmanager
    async def trace_process(self, job: Job) -> AsyncIterator[None]:
        """
        Async context manager for tracing queue consumer job processing,
        capturing performance metrics.

        Args:
            job (Job): The job being processed, containing headers, metadata, and payload.

        Yields:
            None: This context manager does not return a value but captures tracing
                metrics for the job lifecycle.
        """

        if sentry_sdk is None or job.headers is None:
            yield
            return

        if not (sentry_trace_headers := job.sentry_headers()):
            yield
            return

        with (
            sentry_sdk.start_transaction(
                sentry_sdk.continue_trace(
                    sentry_trace_headers,
                    op="function",
                    name="queue_consumer_transaction",
                )
            ) as transaction,
            sentry_sdk.start_span(
                op="queue.process",
                name="queue_consumer",
            ) as span,
        ):
            span.set_data(
                "messaging.message.id",
                job.id,
            )
            span.set_data(
                "messaging.destination.name",
                job.entrypoint,
            )
            span.set_data(
                "messaging.message.body.size",
                len(job.payload or b""),
            )
            span.set_data(
                "messaging.message.receive.latency",
                round((job.updated - job.created).total_seconds() * 1000),
            )

            try:
                yield
                transaction.set_status("ok")
            except Exception:
                transaction.set_status("internal_error")


def set_tracing_class(tracer: TracingProtocol) -> None:
    """
    Sets the tracing instance for the PGQueuer system.

    Args:
        tracer (TracingProtocol): An instance implementing the TracingProtocol.
    """

    TRACER.tracer = tracer
