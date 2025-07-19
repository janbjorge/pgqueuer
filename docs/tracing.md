# Tracing Jobs with Logfire and Sentry

PGQueuer supports distributed tracing through optional integrations with
[Logfire](https://logfire.pydantic.dev/) and
[Sentry](https://docs.sentry.io/).
These tools allow you to visualize job execution and measure performance.

## Installing Optional Dependencies

Both tracing integrations are provided via optional extras. Install them with
`pip` if you want to enable tracing:

```bash
pip install pgqueuer[logfire]
# or
pip install pgqueuer[sentry]
```

You can install both extras simultaneously if you need to switch between them.

## Using Logfire

Configure Logfire before running your producers and consumers. Then register the
tracer with PGQueuer:

```python
import logfire
from pgqueuer import tracing

logfire.configure()
tracing.set_tracing_class(tracing.LogfireTracing())
```

With this setup, trace context is added to job headers when you enqueue
messages. Consumers automatically attach these headers to each span so that
Logfire can correlate producer and consumer activity.

Refer to Logfire's
[manual tracing guide](https://logfire.pydantic.dev/docs/guides/onboarding-checklist/add-manual-tracing/)
for more details about configuring spans and logging.

## Using Sentry

Sentry integration requires initialising the SDK with your DSN. Once configured,
set the tracer class:

```python
import sentry_sdk
from pgqueuer import tracing

sentry_sdk.init(
    dsn="https://<key>@o1.ingest.sentry.io/<project>",
    traces_sample_rate=1.0,
)
tracing.set_tracing_class(tracing.SentryTracing())
```

Job headers will include Sentry tracing information so that consumer spans are
linked to the producing transaction. See the
[Sentry Python documentation](https://docs.sentry.io/platforms/python/)
for advanced configuration options.

## Switching Tracers

Only one tracer can be active at a time. Call
`tracing.set_tracing_class()` with the desired tracer implementation before
starting producers or consumers.
