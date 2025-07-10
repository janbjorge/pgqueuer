# Sentry Telemetry

PGQueuer supports optional integration with Sentry's queues instrumentation. Install the extra dependencies with:

```bash
pip install "pgqueuer[sentry]"
```

Use :class:`pgqueuer.executors.TracedEntrypointExecutor` to instrument job execution and pass a :class:`pgqueuer.telemetry.Telemetry` instance to :meth:`pgqueuer.queries.Queries.enqueue` to record producer spans.

Configure Sentry by providing a DSN when initializing ``sentry_sdk``:

```python
import sentry_sdk

sentry_sdk.init(dsn="https://examplePublicKey@o0.ingest.sentry.io/0")
```

