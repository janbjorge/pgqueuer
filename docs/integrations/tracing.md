# Distributed Tracing

PgQueuer supports distributed tracing through optional integrations with
[Logfire](https://logfire.pydantic.dev/), [Sentry](https://docs.sentry.io/),
and [OpenTelemetry](https://opentelemetry.io/).
These tools allow you to visualize job execution and measure performance across
producer and consumer boundaries.

## Installing optional dependencies

```bash
pip install pgqueuer[logfire]
# or
pip install pgqueuer[sentry]
# or
pip install pgqueuer[opentelemetry]
```

You can install multiple extras simultaneously if you need to switch between them.

## Using Logfire

Configure Logfire before running your producers and consumers, then pass the tracer
to PgQueuer:

```python
import logfire
from pgqueuer import PgQueuer
from pgqueuer.adapters.tracing.logfire import LogfireTracing

logfire.configure()
pgq = PgQueuer.from_asyncpg_connection(connection, tracer=LogfireTracing())
```

With this setup, trace context is added to job headers when you enqueue messages.
Consumers automatically attach these headers to each span so that Logfire can correlate
producer and consumer activity.

Refer to Logfire's
[manual tracing guide](https://logfire.pydantic.dev/docs/guides/onboarding-checklist/add-manual-tracing/)
for more details about configuring spans and logging.

## Using Sentry

Initialise the Sentry SDK with your DSN, then pass the tracer to PgQueuer:

```python
import sentry_sdk
from pgqueuer import PgQueuer
from pgqueuer.adapters.tracing.sentry import SentryTracing

sentry_sdk.init(
    dsn="https://<key>@o1.ingest.sentry.io/<project>",
    traces_sample_rate=1.0,
)
pgq = PgQueuer.from_asyncpg_connection(connection, tracer=SentryTracing())
```

Job headers will include Sentry tracing information so that consumer spans are linked to
the producing transaction. See the
[Sentry Python documentation](https://docs.sentry.io/platforms/python/)
for advanced configuration options.

## Using OpenTelemetry

Configure your `TracerProvider`, then pass the tracer to PgQueuer:

```python
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from pgqueuer import PgQueuer
from pgqueuer.adapters.tracing.opentelemetry import OpenTelemetryTracing

trace.set_tracer_provider(TracerProvider())
pgq = PgQueuer.from_asyncpg_connection(connection, tracer=OpenTelemetryTracing())
```

The adapter follows OTel
[messaging semantic conventions](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/):
`send` spans for enqueue, `process` spans for job execution, with W3C
TraceContext and Baggage propagation through job headers. Compatible with
Jaeger, Datadog, and any OTel-compatible backend.

## Passing a tracer to your own queries

`PgQueuer` gives its tracer to the queue manager and to the queries it builds.
If you build your own `Queries`, give them the tracer, and give `PgQueuer` the
same one so the queue manager traces job processing. `PgQueuer` raises
`ValueError` if its tracer is not the tracer of the queries.

```python
from pgqueuer import PgQueuer
from pgqueuer.adapters.tracing.opentelemetry import OpenTelemetryTracing
from pgqueuer.queries import Queries

tracer = OpenTelemetryTracing()
queries = Queries(driver, tracer=tracer)
pgq = PgQueuer(driver, queries=queries, tracer=tracer)
```

A producer that only enqueues needs just the queries:
`Queries(driver, tracer=tracer)`.

## Switching tracers

Each `PgQueuer` and `Queries` uses the tracer you pass it, so different
producers and consumers in one process can use different tracers.

## Deprecated: `set_tracing_class()`

`set_tracing_class()` from `pgqueuer.ports.tracing` sets one global tracer for
the whole process. It is deprecated, emits a `DeprecationWarning`, and will be
removed in the next major release. It still works until then: a `PgQueuer` or
`Queries` without a tracer of its own uses the global one. Replace it by
passing `tracer=` as shown above.
