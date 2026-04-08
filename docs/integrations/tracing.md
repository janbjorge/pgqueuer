# Distributed Tracing

PGQueuer supports distributed tracing through optional integrations with
[Logfire](https://logfire.pydantic.dev/), [Sentry](https://docs.sentry.io/),
and [OpenTelemetry](https://opentelemetry.io/).
These tools allow you to visualize job execution and measure performance across
producer and consumer boundaries.

## Installing Optional Dependencies

```bash
pip install pgqueuer[logfire]
# or
pip install pgqueuer[sentry]
# or
pip install pgqueuer[opentelemetry]
```

You can install multiple extras simultaneously if you need to switch between them.

## Using Logfire

Configure Logfire before running your producers and consumers, then register the tracer
with PGQueuer:

```python
import logfire
from pgqueuer.adapters import tracing
from pgqueuer.adapters.tracing.logfire import LogfireTracing

logfire.configure()
tracing.set_tracing_class(LogfireTracing())
```

With this setup, trace context is added to job headers when you enqueue messages.
Consumers automatically attach these headers to each span so that Logfire can correlate
producer and consumer activity.

Refer to Logfire's
[manual tracing guide](https://logfire.pydantic.dev/docs/guides/onboarding-checklist/add-manual-tracing/)
for more details about configuring spans and logging.

## Using Sentry

Initialise the Sentry SDK with your DSN, then set the tracer class:

```python
import sentry_sdk
from pgqueuer.adapters import tracing
from pgqueuer.adapters.tracing.sentry import SentryTracing

sentry_sdk.init(
    dsn="https://<key>@o1.ingest.sentry.io/<project>",
    traces_sample_rate=1.0,
)
tracing.set_tracing_class(SentryTracing())
```

Job headers will include Sentry tracing information so that consumer spans are linked to
the producing transaction. See the
[Sentry Python documentation](https://docs.sentry.io/platforms/python/)
for advanced configuration options.

## Using OpenTelemetry

Configure your `TracerProvider`, then register the tracer with PGQueuer:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from pgqueuer.adapters import tracing
from pgqueuer.adapters.tracing.opentelemetry import OpenTelemetryTracing

trace.set_tracer_provider(TracerProvider())
tracing.set_tracing_class(OpenTelemetryTracing())
```

The adapter follows OTel
[messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/):
`send` spans for enqueue, `process` spans for job execution, with W3C
TraceContext and Baggage propagation through job headers. Compatible with
Jaeger, Datadog, and any OTel-compatible backend.

## Switching Tracers

Only one tracer can be active at a time. Call `set_tracing_class()` from
`pgqueuer.adapters.tracing` with the desired tracer implementation before starting
producers or consumers.
