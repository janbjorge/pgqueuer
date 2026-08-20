# PgQueuer vs arq

[arq](https://arq-docs.helpmanual.io/) was for years the default answer to
"asyncio task queue for Python": a small, MIT-licensed library from Samuel Colvin
(of Pydantic) that pairs an asyncio worker with Redis. Its design goals (async
first, minimal dependencies, at-least-once delivery) overlap heavily with
PgQueuer's; the backends differ (Redis vs PostgreSQL).

Facts about arq were checked on 2026-08-20 against the
[official documentation](https://arq-docs.helpmanual.io/),
[repository](https://github.com/python-arq/arq), and arq 0.28.0
([released 2026-04-16](https://pypi.org/project/arq/)).

!!! warning "arq is in maintenance-only mode"
    Since October 2025 the arq README states the project is "in maintenance only
    mode": the maintainers will merge critical security fixes, but users "should not
    expect new bug fixes or features"
    ([python-arq/arq#510](https://github.com/python-arq/arq/issues/510)). The
    modernization roadmap from 2024 (Redis Streams, task DAGs, OpenTelemetry,
    pluggable backends; [#437](https://github.com/python-arq/arq/issues/437)) was
    not implemented. Releases since then have been Python-version bumps. arq still
    works and is widely deployed, but factor its trajectory into a new adoption
    decision.

## Architecture at a glance

| | arq | PgQueuer |
|---|---|---|
| Backend | Redis ≥ 6.2 (TCP, Unix socket, TLS, Sentinel)[^arq-redis] | PostgreSQL |
| Async model | asyncio-native, single-process worker, `max_jobs` concurrent tasks (default 10)[^arq-worker] | asyncio-native, batched dequeue, per-entrypoint and global concurrency limits |
| Delivery | At-least-once: "jobs aren't removed from the queue until they've either succeeded or failed"; "Jobs may be called more than once!"[^arq-delivery] | At-least-once via heartbeat re-pickup ([Reliability](../guides/reliability.md)) |
| Serialization | pickle by default, customizable[^arq-pickle] | Raw `bytes` payload; serialization is yours |
| License / Python | MIT, ≥ 3.9 | MIT, ≥ 3.10 |

[^arq-redis]: [arq docs, connections](https://arq-docs.helpmanual.io/).
[^arq-worker]: [arq docs, worker settings](https://arq-docs.helpmanual.io/).
[^arq-delivery]: [arq docs](https://arq-docs.helpmanual.io/): "arq favours multiple times over zero times"; jobs interrupted by shutdown "remain in the queue to be run again."
[^arq-pickle]: [arq docs, serialization](https://arq-docs.helpmanual.io/): "By default, arq will use the built-in pickle module to serialize and deserialize jobs."

## Feature comparison

| Feature | arq | PgQueuer |
|---------|-----|----------|
| Recurring tasks | `cron()` jobs, `unique=True` by default so multiple workers don't double-fire | [`@pgq.schedule`](../guides/scheduling.md), DB-backed, second-level granularity |
| Retries | `Retry` exception with `defer=`, `max_tries` default 5, `job_try` counter | [`RetryRequested` / `DatabaseRetryEntrypointExecutor`](../guides/retry.md) |
| Deferred jobs | `_defer_by` / `_defer_until` (absolute datetime supported) | [`execute_after`](../guides/deferred-execution.md) (`timedelta` only) |
| Job uniqueness | Custom `_job_id`: same id can't be re-enqueued until the previous run's result clears | [`dedupe_key`](../guides/reliability.md#idempotency) unique constraint while `queued`/`picked` |
| Results | Stored in Redis, `keep_result` default 1 h, `await job.result()` | `pgqueuer_log` audit table + [`CompletionWatcher`](../guides/completion-tracking.md) |
| Cancellation/abort | `Job.abort()`, requires `allow_abort_jobs=True` (off by default) | [Job cancellation](../guides/job-cancellation.md) |
| Transactional enqueue | No; Redis is separate from your database | Yes; same PostgreSQL transaction |
| Monitoring | Redis health-check key + `arq --check`; no dashboard | `pgq dashboard`, [web dashboard](../integrations/web-dashboard.md), [Prometheus](../integrations/prometheus.md), [tracing](../integrations/tracing.md) |
| Maintenance status | Maintenance-only ([#510](https://github.com/python-arq/arq/issues/510)) | Active |

All arq claims: [official docs](https://arq-docs.helpmanual.io/).

## Example: a worker

**arq** functions take a context dict, and the worker is configured by a
`WorkerSettings` class (adapted from the
[arq usage docs](https://arq-docs.helpmanual.io/)):

```python
from arq import cron
from arq.connections import RedisSettings

async def send_email(ctx, to: str):
    ...

class WorkerSettings:
    functions = [send_email]
    redis_settings = RedisSettings()
```

```bash
arq my_module.WorkerSettings
```

**PgQueuer** (see the [Quick Start](../getting-started/quickstart.md)):

```python
@pgq.entrypoint("send_email")
async def send_email(job: Job) -> None:
    ...
```

```bash
pgq run myapp:create_pgq
```

Enqueuing in arq is `await redis.enqueue_job("send_email", "user@example.com")`
([arq usage docs](https://arq-docs.helpmanual.io/)); in PgQueuer,
`await Queries(driver).enqueue("send_email", b"user@example.com")`.

## When arq is the better choice

- You are committed to Redis and don't run PostgreSQL, or your queue must live
  next to other Redis-based infrastructure.
- You want a minimal dependency surface: arq depends only on `redis` and `click`.
- You need absolute-time deferral. `_defer_until` takes a datetime; PgQueuer's
  `execute_after` is relative-only.
- You already run it. arq is stable and has years of production use behind it;
  maintenance-only mode is not by itself a reason to migrate working systems.

## When PgQueuer is the better choice

- Active development: PgQueuer ships features and fixes, while arq accepts critical
  security patches only.
- One fewer service if PostgreSQL is already in your stack.
- Transactional enqueue with your application data.
- Jobs are rows in a WAL-backed relational table with an append-only audit log;
  Redis persistence is tunable but a different discipline.
- arq defaults to pickle, which executes arbitrary code on deserialization if the
  queue is writable by an attacker; PgQueuer's bytes payloads make the
  serialization choice explicit.
- Dashboards, Prometheus metrics, and tracing integrations ship with the project;
  arq's monitoring is a Redis health-check key.
