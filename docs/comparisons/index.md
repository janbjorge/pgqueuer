# Comparisons

PgQueuer is not the right tool for every job. This section compares it with the
Python task queues you are most likely to be choosing between, so you can make an
informed decision, including deciding against PgQueuer.

Each comparison page follows the same rules: every claim about another project
links to its official documentation, repository, or PyPI page (no blog posts, and
no benchmarks we didn't run ourselves); code examples are adapted from each
project's official documentation and link back to the page they came from; facts
were checked on **2026-08-20** against the latest stable release of each project;
and each page has a genuine "when X is the better choice" section.

## The candidates

| | [Celery](celery-comparison.md) | [Procrastinate](procrastinate-comparison.md) | [Dramatiq](dramatiq-comparison.md) | [arq](arq-comparison.md) | PgQueuer |
|---|---|---|---|---|---|
| Backend | RabbitMQ, Redis, SQS[^celery-brokers] | PostgreSQL | RabbitMQ, Redis | Redis | PostgreSQL |
| Async model | Sync-first; no asyncio task support[^celery-async] | Async worker (psycopg 3); sync enqueue supported | Sync-first (processes × threads); opt-in async actors | asyncio-native | asyncio-native; sync enqueue supported |
| Built-in scheduling | Separate `beat` process | `@app.periodic` (DB-deduplicated) | None; APScheduler/Periodiq recommended | `cron()` jobs | `@pgq.schedule` (DB-backed) |
| Workflow primitives | Chains, chords, groups (canvas) | None | Pipelines, groups | None | None |
| Delivery on worker crash | Lost by default (early ack); redelivered with `acks_late` | Manual stalled-job recovery | Redelivered (at-least-once) | Redelivered (at-least-once) | Redelivered via heartbeat timeout (at-least-once) |
| License | BSD-3-Clause | MIT | LGPL-3.0 | MIT | MIT |
| Latest release (2026-08-20)[^releases] | 5.6.3 (2026-03) | 3.9.0 (2026-06) | 2.2.0 (2026-06) | 0.28.0 (2026-04) | 1.3.2 |
| Maintenance status | Active | Active | Active | Maintenance-only[^arq-maint] | Active |
| Python | ≥ 3.9 | ≥ 3.10 | ≥ 3.10 | ≥ 3.9 | ≥ 3.10 |

[^celery-brokers]:
    Celery's [official broker table](https://docs.celeryq.dev/en/stable/getting-started/backends-and-brokers/index.html)
    lists RabbitMQ, Redis, and SQS as stable; Kafka, Zookeeper, and GC Pub/Sub as experimental.
[^celery-async]:
    Celery's concurrency models are prefork, eventlet, gevent, threads, and solo
    ([introduction](https://docs.celeryq.dev/en/stable/getting-started/introduction.html)).
    Native asyncio task support is an open design discussion targeted at Celery 6.0
    ([celery/celery#3884](https://github.com/celery/celery/issues/3884)).
[^arq-maint]:
    arq's README declares the project "in maintenance only mode"; the maintainers accept
    critical security fixes only ([python-arq/arq#510](https://github.com/python-arq/arq/issues/510)).
[^releases]:
    Versions and release dates from PyPI:
    [celery](https://pypi.org/project/celery/),
    [procrastinate](https://pypi.org/project/procrastinate/#history),
    [dramatiq](https://pypi.org/project/dramatiq/),
    [arq](https://pypi.org/project/arq/),
    [pgqueuer](https://pypi.org/project/pgqueuer/).

## How to choose

The short version:

- **You already run PostgreSQL and want one less service** → PgQueuer or
  [Procrastinate](procrastinate-comparison.md). Both use `LISTEN/NOTIFY` and
  `FOR UPDATE SKIP LOCKED`; the comparison page covers where they differ.
- **You need multi-step workflows (fan-out, joins, chains)** →
  [Celery](celery-comparison.md) canvas or [Dramatiq](dramatiq-comparison.md)
  pipelines/groups. PgQueuer is deliberately a job queue, not a workflow engine.
- **You need throughput beyond what one PostgreSQL instance can absorb, or a
  dedicated broker for organizational reasons** → Celery or Dramatiq on RabbitMQ.
- **You want asyncio-native and are committed to Redis** →
  [arq](arq-comparison.md), with the caveat that it is in maintenance-only mode.

## Why these four?

We compare against actively used Python task queues that solve the same problem:
reliable background job processing. Some notable projects are deliberately absent:

- **RQ** and **Huey**: solid Redis-based queues, but the trade-offs mirror the
  Dramatiq and arq pages (external broker, similar delivery semantics), so separate
  pages would repeat the same analysis.
- **Postgres-backed queues in other languages**: if you are not (only) on Python,
  [River](https://riverqueue.com) (Go), [Oban](https://oban.pro) (Elixir),
  [Graphile Worker](https://worker.graphile.org) and
  [pg-boss](https://github.com/timgit/pg-boss) (Node.js), and
  [Solid Queue](https://github.com/rails/solid_queue) (Ruby) validate the same
  Postgres-as-a-queue design in their ecosystems.

For PgQueuer's own throughput characteristics, see [Benchmarks](benchmarks.md), and
run the tool in your environment rather than trusting our numbers.
