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


class OpenTelemetryTracing(TracingProtocol):
    """OpenTelemetry implementation of ``TracingProtocol``.

    Propagates trace context through job headers using the standard
    W3C TraceContext (``traceparent`` / ``tracestate``) and Baggage
    formats via the configured OpenTelemetry propagator.

    Span names and attributes follow the OTel messaging semantic conventions:
    https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/

    Span name format: ``{operation} {destination}``
    e.g. ``send say_hello``, ``process say_hello``

    For single-message enqueue, the ``send`` span IS the creation context and
    its context is propagated into the job headers.  For batch enqueue, a
    ``CLIENT``-kinded ``send`` batch span wraps one ``PRODUCER``-kinded
    ``create {destination}`` span per message; each ``create`` span's context
    is what gets injected.

    For job processing, the ``process`` span uses the message's creation context
    as its parent (valid for single-message processing per the OTel spec).

    The tracer is resolved lazily from the global ``TracerProvider`` at call
    time.  Pass a ``tracer`` to override (useful in tests).

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

        Single message: one ``PRODUCER`` ``send {destination}`` span whose
        context is the creation context injected into the job headers.

        Batch: a ``CLIENT`` ``send`` span (it does not carry a per-message
        creation context) wraps one ``PRODUCER`` ``create {destination}``
        span per message; each create span's context is injected.
        """
        tracer = self._get_tracer()
        if tracer is None:
            for _ in entrypoints:
                yield {}
            return

        if len(entrypoints) == 1:
            # Single-message path — send span IS the creation context.
            entrypoint = entrypoints[0]
            with tracer.start_as_current_span(
                f"send {entrypoint}",
                kind=opentelemetry.trace.SpanKind.PRODUCER,
            ) as span:
                span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_SYSTEM,
                    "pgqueuer",
                )
                span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_DESTINATION_NAME,
                    entrypoint,
                )
                span.set_attribute("messaging.operation.name", "send")
                span.set_attribute("messaging.operation.type", "send")
                carrier: dict[str, str] = {}
                opentelemetry.propagate.inject(carrier)
                yield {"otel": carrier}
        else:
            # Batch path — CLIENT send span + one PRODUCER create span per message.
            with tracer.start_as_current_span(
                "send",
                kind=opentelemetry.trace.SpanKind.CLIENT,
            ) as batch_span:
                batch_span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_SYSTEM,
                    "pgqueuer",
                )
                batch_span.set_attribute("messaging.operation.name", "send")
                batch_span.set_attribute("messaging.operation.type", "send")
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
                            opentelemetry.semconv.trace.SpanAttributes.MESSAGING_SYSTEM,
                            "pgqueuer",
                        )
                        span.set_attribute(
                            opentelemetry.semconv.trace.SpanAttributes.MESSAGING_DESTINATION_NAME,
                            entrypoint,
                        )
                        span.set_attribute("messaging.operation.name", "create")
                        span.set_attribute("messaging.operation.type", "create")
                        carrier = {}
                        opentelemetry.propagate.inject(carrier)
                        yield {"otel": carrier}

    @asynccontextmanager
    async def trace_process(self, job: Job) -> Any:
        """Extract W3C trace context from job headers and wrap execution in a
        ``CONSUMER`` ``process {destination}`` span.

        Uses the message's creation context as the span's parent (valid for
        single-message processing per the OTel spec).  Sets ``STATUS_ERROR``
        and ``error.type`` on exception (re-raises), ``STATUS_OK`` on success.
        """
        tracer = self._get_tracer()
        if tracer is None:
            yield
            return

        otel_headers = job.otel_headers()
        if not otel_headers:
            yield
            return

        ctx = opentelemetry.propagate.extract(otel_headers)
        token = opentelemetry.context.attach(ctx)

        try:
            with tracer.start_as_current_span(
                f"process {job.entrypoint}",
                kind=opentelemetry.trace.SpanKind.CONSUMER,
            ) as span:
                span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_SYSTEM,
                    "pgqueuer",
                )
                span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_DESTINATION_NAME,
                    job.entrypoint,
                )
                span.set_attribute("messaging.operation.name", "process")
                span.set_attribute("messaging.operation.type", "process")
                span.set_attribute(
                    opentelemetry.semconv.trace.SpanAttributes.MESSAGING_MESSAGE_ID,
                    str(job.id),
                )
                # messaging.message.body.size is Opt-In per the OTel spec.
                # The constant is not yet in the installed semconv package so
                # we use the string key directly.
                span.set_attribute(
                    "messaging.message.body.size",
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
                    # error.type is Conditionally Required when the operation fails.
                    span.set_attribute("error.type", type(exc).__qualname__)
                    span.record_exception(exc)
                    raise
        finally:
            opentelemetry.context.detach(token)
