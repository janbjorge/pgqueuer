from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator

from pgqueuer.models import Job


@asynccontextmanager
async def sentry_trace_job(job: Job) -> AsyncGenerator[None, None]:
    """
    Async context manager to instrument Sentry queue consumer metrics.
    Ensures the queue.process span is a direct child of the
    transaction and all required span data is set.
    """
    try:
        import sentry_sdk

        headers = job.headers or {}
        has_sentry_headers = "sentry-trace" in headers or "baggage" in headers

        if not has_sentry_headers:
            yield
            return

        transaction = sentry_sdk.continue_trace(
            headers,
            op="function",
            name="queue_consumer_transaction",
        )

        with (
            sentry_sdk.start_transaction(transaction),
            sentry_sdk.start_span(
                op="queue.process",
                name=f"queue_consumer_transaction:{job.entrypoint}",
            ) as span,
        ):
            # Start the queue.process span as a direct child of the transaction
            # Set all required queue metrics
            span.set_data("messaging.message.id", str(job.id))
            span.set_data("messaging.destination.name", str(job.entrypoint))

            body_size = len(job.payload.decode() if job.payload is not None else "")
            span.set_data("messaging.message.body.size", body_size)

            latency = round((job.updated - job.created).total_seconds() * 1000)
            span.set_data("messaging.message.receive.latency", latency)

            try:
                yield
            except Exception:
                transaction.set_status("internal_error")
                raise

    except ImportError:
        yield
