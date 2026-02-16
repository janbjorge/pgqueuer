from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Generator

try:
    import logfire
    import logfire.propagate
except ImportError:
    logfire = None  # type: ignore[assignment]

from pgqueuer.domain.models import Job
from pgqueuer.ports.tracing import TracingProtocol


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
