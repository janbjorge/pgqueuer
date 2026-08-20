# PgQueuer vs Procrastinate

[Procrastinate](https://procrastinate.readthedocs.io/en/stable/) is PgQueuer's
closest relative: an MIT-licensed, actively maintained Python task queue that also
uses PostgreSQL as its only backend. Both projects build on the same two PostgreSQL
primitives, `LISTEN/NOTIFY` for wake-ups and `FOR UPDATE ... SKIP LOCKED` for
worker coordination, so choosing between them comes down to details, not
architecture.

Facts about Procrastinate were checked on 2026-08-20 against the
[official documentation](https://procrastinate.readthedocs.io/en/stable/),
[repository](https://github.com/procrastinate-org/procrastinate), and Procrastinate
3.9.0 ([released 2026-06-20](https://pypi.org/project/procrastinate/#history)).

## Shared foundation

Both projects, per their own documentation:

- Store jobs in PostgreSQL tables and require no other service.
- Wake workers via [`LISTEN/NOTIFY`](https://procrastinate.readthedocs.io/en/stable/discussions.html) instead of pure polling.
- Claim jobs with `FOR UPDATE ... SKIP LOCKED` so a job is never handed to two workers.
- Run async workers; both let sync code enqueue jobs.
- Ship built-in cron-style periodic tasks with database-enforced deduplication across workers, including optional second-level granularity.
- Support priorities, scheduled (deferred) jobs, retries, cancellation of queued jobs, and aborting running jobs.
- Are MIT-licensed, require Python ≥ 3.10, and see active maintenance in 2026.

## Where they differ

| | Procrastinate | PgQueuer |
|---|---|---|
| Async driver | psycopg 3 (also aiopg); no asyncpg[^proc-drivers] | asyncpg (single or pool) and psycopg 3 |
| Sync usage | Sync connectors can defer jobs only; worker is async-only[^proc-sync] | Sync psycopg driver for enqueue; handlers are async |
| Dequeue | One job per fetch (`LIMIT 1` in `procrastinate_fetch_job_v2`)[^proc-fetch] | Batched dequeue (`batch_size`), see [Performance Tuning](../guides/performance-tuning.md) |
| SQL logic | Stored procedures; the DB owns consistency, which enables non-Python implementations[^proc-sp] | Plain SQL issued by the client library |
| Crash recovery | Manual: wire `get_stalled_jobs()` + `retry_job()` into a periodic task yourself[^proc-stalled] | Automatic re-pickup after `heartbeat_timeout` ([Reliability](../guides/reliability.md)) |
| Serialized execution | [Locks](https://procrastinate.readthedocs.io/en/stable/discussions.html): jobs sharing a lock string run one at a time | [`concurrency_limit`](../guides/concurrency-control.md) per entrypoint (global, SQL-enforced) |
| Duplicate prevention | [`queueing_lock`](https://procrastinate.readthedocs.io/en/stable/howto/advanced/queueing_locks.html): one `todo` job per key | [`dedupe_key`](../guides/reliability.md#idempotency): one `queued`/`picked` job per key, `on_conflict="skip"` for batches |
| Retry configuration | Declarative [`RetryStrategy`](https://procrastinate.readthedocs.io/en/stable/howto/advanced/retry.html) (linear/exponential, per-exception, custom subclass) | [`RetryRequested`](../guides/retry.md) raised from the handler, or `DatabaseRetryEntrypointExecutor` for automatic backoff |
| Deferred jobs | Absolute `schedule_at` or relative `schedule_in`[^proc-schedule] | Relative `execute_after` (`timedelta` only, [docs](../guides/deferred-execution.md)) |
| Django integration | First-class: `procrastinate.contrib.django`, shipped migrations, admin models[^proc-django] | None built in |
| Task arguments | Named tasks with JSON-serialized kwargs | Raw `bytes` payload; serialization is yours |
| Monitoring | CLI admin shell, healthchecks; no web dashboard ("Not yet, maybe someday")[^proc-monitoring] | `pgq dashboard` CLI, [web dashboard](../integrations/web-dashboard.md), [Prometheus](../integrations/prometheus.md), [OpenTelemetry/Logfire/Sentry tracing](../integrations/tracing.md), [MCP server](../integrations/mcp-server.md) |
| PostgreSQL floor | 13+ | 13+ (CI runs 13 through 18) |

[^proc-drivers]: [Connector how-to](https://procrastinate.readthedocs.io/en/stable/howto/basics/connector.html) lists `PsycopgConnector`, `AiopgConnector`, plus sync psycopg/psycopg2/SQLAlchemy connectors.
[^proc-sync]: [Connector how-to](https://procrastinate.readthedocs.io/en/stable/howto/basics/connector.html): "the worker can only be run with an asynchronous connector, but you can defer jobs with either asynchronous and synchronous connectors."
[^proc-fetch]: `procrastinate_fetch_job_v2` in the [shipped schema](https://github.com/procrastinate-org/procrastinate/blob/main/procrastinate/sql/schema.sql) selects with `ORDER BY jobs.priority DESC, jobs.id ASC LIMIT 1 FOR UPDATE OF jobs SKIP LOCKED`.
[^proc-sp]: [Discussions](https://procrastinate.readthedocs.io/en/stable/discussions.html): logic lives in stored procedures "so that the database is solely responsible for consistency."
[^proc-stalled]: [Retry stalled jobs](https://procrastinate.readthedocs.io/en/stable/howto/production/retry_stalled_jobs.html): workers heartbeat every 10 s, but recovering stalled jobs requires scheduling `get_stalled_jobs()` + `retry_job()` yourself, e.g. as an `@app.periodic` task.
[^proc-schedule]: [Schedule how-to](https://procrastinate.readthedocs.io/en/stable/howto/advanced/schedule.html).
[^proc-django]: [Django configuration](https://procrastinate.readthedocs.io/en/stable/howto/django/configuration.html) and [models](https://procrastinate.readthedocs.io/en/stable/howto/django/models.html) (read-only ORM models, exposed in Django admin).
[^proc-monitoring]: [Monitoring how-to](https://procrastinate.readthedocs.io/en/stable/howto/production/monitoring.html).

## Example: defining and deferring a task

**Procrastinate** tasks take named arguments and are deferred via the task object
(adapted from the
[quickstart](https://procrastinate.readthedocs.io/en/stable/quickstart.html)):

```python
# procrastinate_app.py
from procrastinate import App, PsycopgConnector

app = App(connector=PsycopgConnector())

@app.task(queue="emails")
def send_email(to: str, subject: str):
    ...

# defer from sync or async code
send_email.defer(to="user@example.com", subject="Hi")
```

```bash
procrastinate --app=procrastinate_app.app worker
```

**PgQueuer** entrypoints receive a `Job` with a raw payload; you choose the
serialization (see the [Quick Start](../getting-started/quickstart.md)):

```python
# pgqueuer_app.py
import json
from contextlib import asynccontextmanager
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.models import Job

@asynccontextmanager
async def create_pgq():
    conn = await asyncpg.connect()
    pgq = PgQueuer.from_asyncpg_connection(conn)

    @pgq.entrypoint("send_email")
    async def send_email(job: Job) -> None:
        data = json.loads(job.payload)
        ...

    yield pgq
```

```python
await Queries(driver).enqueue("send_email", json.dumps({"to": "user@example.com"}).encode())
```

```bash
pgq run pgqueuer_app:create_pgq
```

Procrastinate's named-kwargs model is more ergonomic; PgQueuer's bytes payload is
more explicit and serialization-agnostic. Which you prefer is a taste question.

## When Procrastinate is the better choice

- Django projects. Shipped Django migrations, ORM models, and admin integration
  make it the natural Postgres queue for Django; PgQueuer has no Django integration.
- Declarative retry policies: per-exception retry strategies with linear or
  exponential wait, or fully custom `RetryDecision` logic that can even change the
  job's queue or priority on retry.
- Lock groups. Serializing arbitrary groups of jobs by lock string (say, one lock
  per customer) is more flexible than per-entrypoint concurrency limits.
- Absolute-time scheduling. `schedule_at` takes a datetime; PgQueuer's
  `execute_after` only accepts a relative `timedelta`.
- You may want non-Python consumers someday. Keeping queue logic in stored
  procedures is an explicit design goal to allow other-language implementations.

## When PgQueuer is the better choice

- Throughput. Batched dequeue plus the asyncpg driver are the basis of the numbers
  in [Benchmarks](benchmarks.md), measured with our own tool, so verify in your
  environment. Procrastinate fetches one job per query and publishes no benchmark
  figures, so we make no comparative claim; run both against your workload if
  throughput is the deciding factor.
- Automatic crash recovery. Stalled jobs are re-picked after `heartbeat_timeout`
  with no extra wiring; in Procrastinate you build that periodic task yourself.
- Observability. Terminal and web dashboards, Prometheus metrics, and
  OpenTelemetry/Logfire/Sentry tracing ship as integrations.
- asyncpg. If your stack is already on asyncpg (or you want its performance
  profile), note that Procrastinate's async path is psycopg 3 only.
- Batch operations. Batched enqueue with per-item `dedupe_key` conflict handling
  (`on_conflict="skip"`) and batched dequeue are built in.

Both are good choices. If you are torn: teams deep in Django tend toward
Procrastinate; teams optimizing for throughput, observability, or asyncpg tend
toward PgQueuer.
