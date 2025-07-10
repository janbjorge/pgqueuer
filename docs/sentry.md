# Sentry Telemetry

PGQueuer supports optional integration with Sentry's queues instrumentation. Install the extra dependencies with:

```bash
pip install "pgqueuer[sentry]"
```

Once installed, job and schedule execution will automatically create spans when using `QueueManager` and `SchedulerManager`. These spans are grouped under the operations `pgqueuer.job` and `pgqueuer.schedule`.

