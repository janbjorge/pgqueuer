from __future__ import annotations

from contextlib import asynccontextmanager
from typing import TYPE_CHECKING, AsyncIterator, Generator

if TYPE_CHECKING:
    import logfire
    import logfire.propagate

try:
    import logfire  # noqa: F811
    import logfire.propagate  # noqa: F811

    HAS_LOGFIRE = True
except ImportError:
    HAS_LOGFIRE = False

from pgqueuer.domain.models import Job
from pgqueuer.ports.tracing import TracingProtocol


class LogfireTracing(TracingProtocol):
    def trace_publish(self, entrypoints: list[str]) -> Generator[dict, None, None]:
        if not HAS_LOGFIRE:
            # One header per entrypoint: merge_tracing_headers zips against
            # the entrypoint list with strict=True.
            for _ in entrypoints:
                yield {}
            return

        with logfire.span("Enqueue"):
            for entrypoint in entrypoints:
                with logfire.span("Enqueued: {entrypoint=}", entrypoint=entrypoint):
                    yield {"logfire": dict(logfire.propagate.get_context())}

    @asynccontextmanager
    async def trace_process(self, job: Job) -> AsyncIterator[None]:
        """Wrap consumer processing of *job* in a Logfire span when SDK + headers present."""
        if not HAS_LOGFIRE or job.headers is None:
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
