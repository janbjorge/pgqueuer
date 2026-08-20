# PgQueuer vs Dramatiq

[Dramatiq](https://dramatiq.io) is a broker-based task queue with a strong focus on
reliability: at-least-once delivery, automatic retries with exponential backoff, and
a dead-letter queue are the defaults, not opt-ins. It is actively maintained
([2.2.0 released 2026-06-17](https://pypi.org/project/dramatiq/)) and positions
itself as a simpler, more reliable
alternative to Celery ([motivation](https://dramatiq.io/motivation.html)).

The structural difference from PgQueuer is the same as with Celery: Dramatiq needs a
message broker (RabbitMQ or Redis), while PgQueuer needs only the PostgreSQL you
already run. Facts about Dramatiq were checked on 2026-08-20 against
[dramatiq.io](https://dramatiq.io) and Dramatiq 2.2.0.

## Architecture at a glance

| | Dramatiq | PgQueuer |
|---|---|---|
| Message transport | RabbitMQ (default) or Redis[^brokers] | PostgreSQL |
| Result storage | Opt-in `Results` middleware: Redis or Memcached backend[^results] | PostgreSQL (`pgqueuer_log`) |
| Worker model | Processes × threads (default: one process per core, 8 threads each)[^worker] | asyncio tasks in one process; scale with more processes |
| Async support | Opt-in `AsyncIO` middleware runs `async def` actors on an event-loop thread[^async] | asyncio-native |
| Delivery | At-least-once; idempotence documented as a requirement[^delivery] | At-least-once via heartbeat re-pickup ([Reliability](../guides/reliability.md)) |
| License | LGPL-3.0[^license] | MIT |
| Python | ≥ 3.10 | ≥ 3.10 |

[^brokers]: [API reference, brokers](https://dramatiq.io/reference.html).
[^results]: [API reference, results](https://dramatiq.io/reference.html); the `Results` middleware is "available, but not enabled by default."
[^worker]: [User guide, workers](https://dramatiq.io/guide.html).
[^async]: Async actors coexist with sync ones ([cookbook](https://dramatiq.io/cookbook.html)), but concurrency per worker process is still bounded by the worker thread count, since each thread waits on its async actor's result ([Bogdanp/dramatiq#536](https://github.com/Bogdanp/dramatiq/pull/536)).
[^delivery]: [Best practices](https://dramatiq.io/best_practices.html): "Dramatiq actors may receive the same message multiple times in the event of a worker failure."
[^license]: [PyPI](https://pypi.org/project/dramatiq/): "GNU Lesser General Public License v3 or later (LGPLv3+)."

## Feature comparison

| Feature | Dramatiq | PgQueuer |
|---------|----------|----------|
| Retries | Automatic, exponential backoff, `max_retries` default 20, `retry_when` predicate[^retries] | [`RetryRequested` / `DatabaseRetryEntrypointExecutor`](../guides/retry.md); explicit, not automatic by default |
| Dead-letter queue | Built in; exhausted messages are kept up to 7 days[^dlq] | No DLQ; [`on_failure="hold"`](../guides/hold-failed-jobs.md) keeps failed jobs for `pgq requeue` |
| Recurring tasks | None built in; official recommendation is APScheduler or Periodiq[^cron] | Built-in [`@pgq.schedule`](../guides/scheduling.md) |
| Delayed messages | `delay=` in milliseconds; docs warn "your message broker is not a database"[^delay] | [`execute_after`](../guides/deferred-execution.md); jobs are database rows, so long delays are fine |
| Transactional enqueue | No; the broker is separate from your database | Yes; same PostgreSQL transaction |
| Rate limiting | `BucketRateLimiter`, `ConcurrentRateLimiter`, `WindowRateLimiter` (needs Redis/Memcached)[^rate] | None built in |
| Composition | Pipelines (`\|` operator), groups, callbacks[^compose] | None; single jobs only |
| Middleware | First-class extension system[^middleware] | [Custom executors](../guides/custom-executors.md) |
| Priorities | Per-actor, affects message selection only[^prio2] | Integer priority column, ordered in dequeue SQL |
| Deduplication | Not built in | [`dedupe_key`](../guides/reliability.md#idempotency) |
| Monitoring | Prometheus middleware (opt-in extra since 2.0), no official dashboard[^prom] | `pgq dashboard`, [web dashboard](../integrations/web-dashboard.md), [Prometheus](../integrations/prometheus.md), [tracing](../integrations/tracing.md) |
| Windows support | Yes[^windows] | Supported (event-loop caveats: see [Troubleshooting](../development/troubleshooting.md)) |

[^retries]: [User guide, retries](https://dramatiq.io/guide.html).
[^dlq]: [User guide](https://dramatiq.io/guide.html): messages exceeding retry/age limits move to the DLQ "where it's kept for up to 7 days and then automatically dropped."
[^cron]: [Motivation](https://dramatiq.io/motivation.html) ("Cronlike scheduling: No") and [cookbook](https://dramatiq.io/cookbook.html) ("APScheduler is the recommended scheduler to use with Dramatiq").
[^delay]: [User guide, scheduling messages](https://dramatiq.io/guide.html): "Scheduled messages should represent a small subset of all your messages."
[^rate]: [API reference, rate limiters](https://dramatiq.io/reference.html).
[^compose]: [Cookbook, composition](https://dramatiq.io/cookbook.html). Complex orchestration is delegated to the external `dramatiq-workflow` package ([advanced](https://dramatiq.io/advanced.html)).
[^middleware]: [Advanced, middleware](https://dramatiq.io/advanced.html).
[^prio2]: [User guide, prioritizing messages](https://dramatiq.io/guide.html): priority "only takes effect when Dramatiq is choosing which message to run."
[^prom]: [Advanced, Prometheus](https://dramatiq.io/advanced.html); Prometheus middleware moved out of the defaults in 2.0.0 ([changelog](https://dramatiq.io/changelog.html)).
[^windows]: [Motivation, comparison table](https://dramatiq.io/motivation.html).

## Example: an actor vs an entrypoint

**Dramatiq** (adapted from the [user guide](https://dramatiq.io/guide.html)):

```python
import dramatiq

@dramatiq.actor(max_retries=5)
def send_email(to, subject):
    ...

send_email.send("user@example.com", "Hi")
```

```bash
dramatiq my_module
```

**PgQueuer** (see the [Quick Start](../getting-started/quickstart.md)):

```python
@pgq.entrypoint("send_email")
async def send_email(job: Job) -> None:
    data = json.loads(job.payload)
    ...
```

```python
await Queries(driver).enqueue("send_email", json.dumps({"to": "user@example.com"}).encode())
```

```bash
pgq run myapp:create_pgq
```

## When Dramatiq is the better choice

- Your codebase is sync and CPU-hungry: the processes × threads model uses all
  cores without asyncio, while PgQueuer requires `async def` handlers.
- You want reliability without configuration. Automatic retries with exponential
  backoff and a dead-letter queue are the defaults; PgQueuer makes retries explicit
  and has no DLQ.
- You need rate limiting or distributed mutexes; the built-in limiter classes cover
  cases PgQueuer doesn't.
- Pipelines and groups handle chained or fan-out work.
- A broker is already part of your platform. Dramatiq uses RabbitMQ well (HA queues,
  failover, virtual-host multi-tenancy).

## When PgQueuer is the better choice

- No broker to run. One fewer service, and note that Redis is a hard dependency for
  Dramatiq's result backend and rate limiters even on the RabbitMQ broker.
- Transactional enqueue: enqueue in the same transaction as your application data,
  where a broker needs an outbox pattern.
- asyncio-native workloads. Dramatiq's async actors run on a side event loop and
  remain capped by the worker thread count; PgQueuer schedules coroutines directly.
- Recurring jobs need no third-party scheduler process.
- Jobs are rows, so the audit log, statistics, and ad-hoc SQL queries come free,
  plus the shipped dashboards.
- MIT vs LGPL-3.0 licensing. Usually irrelevant for using a library, but some
  organizations have policies about copyleft dependencies.
