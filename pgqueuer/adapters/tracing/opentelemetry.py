from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any, Generator

try:
    import opentelemetry
    import opentelemetry.context
    import opentelemetry.propagate
    import opentelemetry.semconv.trace
    import opentelemetry.trace
except ImportError:
    opentelemetry = None  # type: ignore[assignment]

from pgqueuer.domain.models import Job
from pgqueuer.ports.tracing import TracingProtocol

# Canonical messaging.system value for PgQueuer spans.
_MESSAGING_SYSTEM = "pgqueuer"


class OpenTelemetryTracing(TracingProtocol):
    """OpenTelemetry implementation of ``TracingProtocol``.

    Propagates trace context through job headers using the standard
    W3C TraceContext (``traceparent`` / ``tracestate``) and Baggage
    formats via the configured OpenTelemetry propagator.

    Span names and attributes follow the OTel messaging semantic conventions:
    https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/

    Span name format: ``{operation} {destination}``
    e.g. ``send say_hello``, ``process say_hello``

    The tracer is resolved lazily from the global ``TracerProvider``
    at call time.  Pass a ``tracer`` to override (useful in tests).

    Usage::

        from opentelemetry import trace
        from opentelemetry.sdk.trace import TracerProvider
        from opentelemetry.sdk.trace.export import SimpleSpanProcessor
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

        exporter = InMemorySpanExporter()
        provider = TracerProvider()
        provider.add_span_processor(SimpleSpanProcessor(exporter))
        trace.set_tracer_provider(provider)

        pgq = PgQueuer(driver, tracer=OpenTelemetryTracing())
    """

    def __init__(
        self,
        tracer: Any = None,
        instrumentation_name: str = "pgqueuer",
    ) -> None:
        self._tracer_arg = tracer
        self._instrumentation_name = instrumentation_name

    def _get_tracer(self) -> Any:
        if opentelemetry is None:
            return None
        if self._tracer_arg is not None:
            return self._tracer_arg
        return opentelemetry.trace.get_tracer(self._instrumentation_name)

    def trace_publish(self, entrypoints: list[str]) -> Generator[dict, None, None]:
        """Inject W3C trace context into job headers on enqueue.

        For a single entrypoint a ``send`` span (``PRODUCER``) is created and
        its context is injected directly into the job headers.

        For multiple entrypoints a batch ``send`` span (``CLIENT`` — it does
        not carry a per-message creation context) is opened, and one ``create``
        span (``PRODUCER``) is created per entrypoint; the creation context
        from each ``create`` span is injected into that job's headers.

        Span names follow ``{operation} {destination}`` from the OTel
        messaging semantic conventions.
        """
        tracer = self._get_tracer()
        if tracer is None:
            for _ in entrypoints:
                yield {}
            return

        single = len(entrypoints) == 1

        if not single:
            # Batch path: CLIENT-kinded send span wraps all per-message create spans.
            with tracer.start_as_current_span(
                "send",
                kind=opentelemetry.trace.SpanKind.CLIENT,
            ) as batch_span:
                batch_span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_SYSTEM,
                    _MESSAGING_SYSTEM,
                )
                batch_span.set_attribute("messaging.operation.name", "send")
                batch_span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_BATCH_MESSAGE_COUNT,
                    len(entrypoints),
                )
                for entrypoint in entrypoints:
                    with tracer.start_as_current_span(
                        f"create {entrypoint}",
                        kind=opentelemetry.trace.SpanKind.PRODUCER,
                    ) as span:
                        span.set_attribute(
                            opentelemetry.semconv.trace.SpanAttributes.MESSAGING_DESTINATION_NAME,
                            entrypoint,
                        )
                        span.set_attribute(
                            opentelemetry.semconv.trace.SpanAttributes.MESSAGING_SYSTEM,
                            _MESSAGING_SYSTEM,
                        )
                        span.set_attribute("messaging.operation.name", "create")
                        carrier: dict[str, str] = {}
                        opentelemetry.propagate.inject(carrier)
                        yield {"otel": carrier}
        else:
            # Single-message path: the send span IS the creation context.
            entrypoint = entrypoints[0]
            with tracer.start_as_current_span(
                f"send {entrypoint}",
                kind=opentelemetry.trace.SpanKind.PRODUCER,
            ) as span:
                span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_DESTINATION_NAME,
                    entrypoint,
                )
                span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_SYSTEM,
                    _MESSAGING_SYSTEM,
                )
                span.set_attribute("messaging.operation.name", "send")
                carrier = {}
                opentelemetry.propagate.inject(carrier)
                yield {"otel": carrier}

    @asynccontextmanager
    async def trace_process(self, job: Job) -> Any:
        """Extract W3C trace context from job headers and wrap execution
        in a ``process`` ``CONSUMER`` span.

        The span is set to ``STATUS_ERROR`` on exception (and re-raises),
        ``STATUS_OK`` on clean completion.

        Span name follows ``process {destination}`` from the OTel messaging
        semantic conventions.
        """
        tracer = self._get_tracer()
        if tracer is None or job.headers is None:
            yield
            return

        otel_headers = job.otel_headers()
        if not otel_headers:
            yield
            return

        # Restore the producer creation context so the process span is a
        # child of the message's creation span (valid for single-message
        # processing per the OTel spec).
        ctx = opentelemetry.propagate.extract(otel_headers)
        token = opentelemetry.context.attach(ctx)

        try:
            with tracer.start_as_current_span(
                f"process {job.entrypoint}",
                kind=opentelemetry.trace.SpanKind.CONSUMER,
            ) as span:
                span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_DESTINATION_NAME,
                    job.entrypoint,
                )
                span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_SYSTEM,
                    _MESSAGING_SYSTEM,
                )
                span.set_attribute("messaging.operation.name", "process")
                span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_MESSAGE_ID,
                    str(job.id),
                )
                span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_MESSAGE_PAYLOAD_SIZE_BYTES,
                    len(job.payload or b""),
                )

                try:
                    yield
                    span.set_status(opentelemetry.trace.StatusCode.OK)
                except Exception as exc:
                    span.set_status(
                        opentelemetry.trace.StatusCode.ERROR,
                        description=str(exc),
                    )
                    span.record_exception(exc)
                    raise
        finally:
            opentelemetry.context.detach(token)
