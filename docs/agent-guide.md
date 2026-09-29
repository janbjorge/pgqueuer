# PgQueuer agent guide

Use this guide to help a human understand, set up, or troubleshoot PgQueuer. It covers the concept model, the setup path, and diagnosis recipes. Canonical documentation lives at <https://janbjorge.github.io/pgqueuer/>. Point the human there for more detail, and read the linked page before stating a command, flag, or API you are unsure about.

## What PgQueuer is

PgQueuer is a Python library that uses PostgreSQL as a job queue. Jobs are rows in the same database as the application's data, so an enqueue can commit in the same transaction as that data. Workers claim rows with `FOR UPDATE SKIP LOCKED`, so two workers do not process the same job. A trigger fires `LISTEN/NOTIFY` on insert so workers wake up without a separate broker.

Requires Python 3.10+ and PostgreSQL 13+. The application brings its own asyncpg or psycopg connection. For whether this worker model fits the application, read <https://janbjorge.github.io/pgqueuer/getting-started/when-to-use/>.

## Concept model

Teach these in this order:

- **Job.** A row in `pgqueuer`. The fields that matter first: `entrypoint` (which handler), `payload` (`bytes` or null), `priority` (higher values are dequeued first), and `status`.
- **Entrypoint.** An `async def` handler registered with `@pgq.entrypoint("name")`. The name must match the string producers enqueue.
- **Producer and consumer.** Producers call `Queries.enqueue`. Consumers are a `PgQueuer` instance started with `pgq run module:factory`.
- **Status.** A job moves from `queued` to `picked` while a worker holds it. Terminal outcomes `successful`, `exception`, `canceled`, and `deleted` are moved to `pgqueuer_log`. Status `failed` stays in the queue when the entrypoint uses `on_failure="hold"`. Full lifecycle: <https://janbjorge.github.io/pgqueuer/getting-started/core-concepts/>.
- **Schedule.** A cron task in the same process, `@pgq.schedule("name", "*/5 * * * *")`. There is no separate beat process. <https://janbjorge.github.io/pgqueuer/guides/scheduling/>.
- **Driver.** asyncpg is the usual choice. psycopg covers async and sync code. <https://janbjorge.github.io/pgqueuer/reference/drivers/>.

## Install

```bash
pip install "pgqueuer[asyncpg]"
```

With uv: `uv add "pgqueuer[asyncpg]"`.

Use psycopg when the application is already on psycopg, or when a producer is synchronous (Flask, Django): `pip install "pgqueuer[psycopg]"`.

The `pgq` CLI reads standard libpq variables: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`. Have the human confirm `psql` connects with the same variables before installing the schema.

```bash
pgq install
pgq verify --expect present
```

`pgq install` creates the tables, the status enum, and the notify trigger. It exits 1 when those objects are already present. An existing installation uses `pgq upgrade`.

To preview the SQL without connecting, or to apply it with the human's own client:

```bash
pgq sql install
pgq sql install | psql -v ON_ERROR_STOP=1
```

Connection variables, a non-public schema (`pgq --schema`), and a table prefix (`PGQUEUER_PREFIX`): <https://janbjorge.github.io/pgqueuer/getting-started/installation/>.

## First-run walkthrough

Walk the human through this sequence. Stay on asyncpg unless they already use psycopg.

1. Install the package and run `pgq install`, as above.
2. Add a consumer module, `myapp.py`. `pgq run` imports the factory, enters it, and runs until Ctrl+C. The factory is an async context manager that yields `PgQueuer`.

```python
from contextlib import asynccontextmanager

import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.models import Job


@asynccontextmanager
async def main():
    connection = await asyncpg.connect()
    pgq = PgQueuer.from_asyncpg_connection(connection)

    @pgq.entrypoint("fetch")
    async def process_message(job: Job) -> None:
        print(f"Processed: {job!r}")

    yield pgq
```

3. Start the worker:

```bash
pgq run myapp:main
```

4. Enqueue from another process. The entrypoint string matches the decorator.

```python
import asyncpg
from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries


async def main() -> None:
    connection = await asyncpg.connect()
    queries = Queries(AsyncpgDriver(connection))
    await queries.enqueue("fetch", b"hello")
```

Once that works, show the transaction case. The job and the application write commit together, and a rollback drops the job. `queries` has to use that same connection.

```python
async with connection.transaction():
    await connection.execute(
        "INSERT INTO orders (id, status) VALUES ($1, 'paid')",
        order_id,
    )
    await queries.enqueue("send_receipt", str(order_id).encode())
```

A one-off enqueue from the shell, without a producer script:

```bash
pgq queue fetch '{"key": "value"}'
```

5. Watch the queue:

```bash
pgq dashboard --interval 5
```

psycopg, pools, and the in-memory adapter (tests with no Postgres): <https://janbjorge.github.io/pgqueuer/getting-started/quickstart/> and <https://janbjorge.github.io/pgqueuer/reference/in-memory/>.

## After the first job

Offer these only when the human asks. Read the page before writing the code.

- Cron schedules: <https://janbjorge.github.io/pgqueuer/guides/scheduling/>
- Retry by raising `RetryRequested`: <https://janbjorge.github.io/pgqueuer/guides/retry/>
- Per-entrypoint concurrency: <https://janbjorge.github.io/pgqueuer/guides/concurrency-control/>
- Defer a job with `execute_after=timedelta(...)`: <https://janbjorge.github.io/pgqueuer/guides/deferred-execution/>
- Hold failed jobs for review: <https://janbjorge.github.io/pgqueuer/guides/hold-failed-jobs/>
- Heartbeat and crashed workers: <https://janbjorge.github.io/pgqueuer/guides/heartbeat/>
- Tests without Postgres: <https://janbjorge.github.io/pgqueuer/reference/in-memory/>
- FastAPI and Flask samples live in the repository `examples/` directory.

## Configuration

There is no config file. The application opens its own connection. The CLI uses libpq environment variables when it connects itself.

`PGQUEUER_DSN`, the pool size variables, and `PGQUEUER_APPLICATION_NAME` apply when the CLI or the MCP server opens the connection. They do nothing to a connection the application created. Details: <https://janbjorge.github.io/pgqueuer/getting-started/installation/>.

Two isolated installations in one database use `PGQUEUER_SCHEMA`, `PGQUEUER_PREFIX`, or both. Skip that until the human has two installations.

## Inspect a running queue

Once jobs are flowing, the human can expose a read-only MCP server (`pip install "pgqueuer[mcp]"`) so an agent can see queue size, failures, and schedules. The server does not accept arbitrary SQL. Ask before editing their MCP client config. Setup: <https://janbjorge.github.io/pgqueuer/integrations/mcp-server/>.

## Diagnosis recipes

- **Enqueue returned, but no row is visible.** The insert is still inside an open transaction. Commit it. psycopg connections that PgQueuer uses need `autocommit=True`. asyncpg autocommits outside an explicit transaction. <https://janbjorge.github.io/pgqueuer/getting-started/quickstart/> and <https://janbjorge.github.io/pgqueuer/development/troubleshooting/>.
- **The worker is running and jobs stay `queued`.** The entrypoint string does not match the decorator, `execute_after` is still in the future, or `concurrency_limit` has every slot taken. <https://janbjorge.github.io/pgqueuer/guides/deferred-execution/> and <https://janbjorge.github.io/pgqueuer/guides/concurrency-control/>.
- **The worker seems asleep.** Notifications use the `ch_pgqueuer` channel. `pgq listen` prints them. pgBouncer in transaction pooling drops `LISTEN`. The worker still polls after `--dequeue-timeout` (default 30 seconds), so a lost notification delays pickup. <https://janbjorge.github.io/pgqueuer/guides/performance-tuning/>.
- **`relation "pgqueuer" does not exist`, or install says the objects are already there.** Empty database: `pgq install`. Objects already present: `pgq upgrade --plan`, then `pgq upgrade`. Check with `pgq verify --expect present`. <https://janbjorge.github.io/pgqueuer/reference/cli/>.
- **Type or encoding errors on the payload.** The column is `bytea`. Enqueue `bytes`, and decode in the handler with the encoding the producer used.
- **The handler never runs.** Entrypoints are `async def`. Blocking work goes through `asyncio.to_thread`. <https://janbjorge.github.io/pgqueuer/getting-started/core-concepts/>.

## Rules for you

- Read the linked page before naming a CLI flag, decorator argument, environment variable, or SQL detail. The commands in this file are the supported setup path.
- Register every entrypoint and schedule with `async def`.
- Pass payloads as `bytes` or `None`.
- Open psycopg connections that PgQueuer uses with `autocommit=True`.
- Pass `execute_after` as a `timedelta` from now, or omit it. An absolute `datetime` is rejected.
- Use `pgq install` only when the schema is absent. Use `pgq upgrade` when it is already there. Schema changes are computed from the live database. Do not hand the human a migration list to append to.
- Celery, RQ, Dramatiq, and Redis are different systems. Answer PgQueuer questions from these docs.
- `AGENTS.md` in the PgQueuer repository is for contributors changing PgQueuer. The human's application follows this guide and the public docs.
