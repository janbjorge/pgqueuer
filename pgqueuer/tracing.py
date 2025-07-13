from __future__ import annotations

from contextlib import asynccontextmanager
from typing import AsyncGenerator, Generator

from pgqueuer.models import Job

try:
    import sentry_sdk
except ImportError:
    sentry_sdk = None  # type: ignore[assignment]


def sentry_trace_publish(
    entryponits: list[str],
    body_sizes: list[int],
) -> Generator[dict | None, None, None]:
    """
    Publishes Sentry tracing headers for queue producer operations.

    This generator yields Sentry trace headers for each entrypoint and body size pair,
    allowing distributed tracing of messages published to the queue. If Sentry SDK is not
    available, yields empty dictionaries.

    References:
        - https://docs.sentry.io/platforms/python/guides/asyncio/performance/instrumentation/
        - https://docs.sentry.io/platforms/python/performance/instrumentation/custom-instrumentation/
    """
    if sentry_sdk is None:
        yield from [None] * len(entryponits)
        return

    with sentry_sdk.start_transaction(
        op="function", name="queue_producer_transaction"
    ) as transaction:
        for entryponit, body_size in zip(entryponits, body_sizes, strict=True):
            with sentry_sdk.start_span(
                op="queue.publish", name=f"queue_producer:{entryponit}"
            ) as span:
                # Set span data
                span.set_data("messaging.destination.name", entryponit)
                span.set_data("messaging.message.body.size", body_size)

                yield {
                    "pgq-sentry-trace": {
                        "sentry-trace": sentry_sdk.get_traceparent(),
                        "baggage": sentry_sdk.get_baggage(),
                    }
                }

        transaction.set_status("ok")


@asynccontextmanager
async def sentry_trace_process(job: Job) -> AsyncGenerator[None, None]:
    """
    Async context manager for Sentry tracing of queue consumer job processing.

    This context manager continues a Sentry trace from incoming job headers (if present)
    and creates a transaction and span for processing the job. It sets relevant Sentry
    span data for distributed tracing and queue metrics, including message ID, destination,
    body size, and receive latency. The transaction status is set based on success or failure.

    References:
        - https://docs.sentry.io/platforms/python/guides/asyncio/performance/instrumentation/
        - https://docs.sentry.io/platforms/python/performance/instrumentation/custom-instrumentation/
        - https://docs.sentry.io/platforms/python/guides/asyncio/enriching-events/context/
    """

    if sentry_sdk is None:
        yield
        return

    headers = job.headers or {}
    sentry_headers = headers.get("pgq-sentry-trace", {})

    if not sentry_headers:
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
        span.set_data("messaging.message.id", f"pgq-job-id: {job.id!s}")
        span.set_data("messaging.destination.name", str(job.entrypoint))

        body_size = len(job.payload.decode()) if job.payload is not None else 0
        span.set_data("messaging.message.body.size", body_size)

        latency = round((job.updated - job.created).total_seconds() * 1000)
        span.set_data("messaging.message.receive.latency", latency)

        try:
            yield
        except Exception:
            transaction.set_status("internal_error")
            raise
        else:
            transaction.set_status("ok")
