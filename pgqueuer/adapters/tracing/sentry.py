from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncIterator, Generator

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None  # type: ignore[assignment]

from pgqueuer.domain.models import Job
from pgqueuer.ports.tracing import TracingProtocol


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
