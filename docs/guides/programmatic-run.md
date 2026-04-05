# Running PgQueuer Programmatically

The `pgq run` CLI command is the standard way to start a worker, but sometimes you
need to start PgQueuer from Python directly -- for example, when embedding it in a
larger application, writing integration tests, or building custom orchestration.

PgQueuer provides an async `run()` function that mirrors the CLI `run` command
without requiring Typer or any CLI dependency.

---

## Standalone Worker

```python
import asyncio
import pgqueuer

asyncio.run(pgqueuer.run("myapp:main"))
```

This is equivalent to `pgq run myapp:main` -- it imports the factory, sets up signal
handlers, and starts the supervisor loop.

---

## Using a Callable Factory

Instead of a `"module:function"` string, you can pass a callable directly:

```python
import asyncio
import asyncpg
from contextlib import asynccontextmanager
from pgqueuer import PgQueuer, run
from pgqueuer.models import Job

@asynccontextmanager
async def create_pgqueuer():
    conn = await asyncpg.connect()
    try:
        pgq = PgQueuer.from_asyncpg_connection(conn)

        @pgq.entrypoint("process")
        async def handler(job: Job) -> None:
            print(f"Processing {job!r}")

        yield pgq
    finally:
        await conn.close()

asyncio.run(run(create_pgqueuer))
```

---

## Embedding in an Existing Async Application

Since `run()` is async, you can embed it directly in an existing event loop:

```python
import asyncio
from pgqueuer import run

async def main():
    shutdown = asyncio.Event()

    # Start PgQueuer in a background task
    task = asyncio.create_task(
        run("myapp:main", shutdown=shutdown)
    )

    # ... do other work ...

    # Signal PgQueuer to stop
    shutdown.set()
    await task

asyncio.run(main())
```

The `shutdown` parameter lets you control when PgQueuer stops from outside.

---

## Parameter Reference

All parameters after `factory` are keyword-only.

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `factory` | `str \| Callable` | *(required)* | `"module:function"` string or a callable factory |
| `dequeue_timeout` | `float` | `30.0` | Max seconds to wait for new jobs per dequeue cycle |
| `batch_size` | `int` | `10` | Number of jobs to pull per batch |
| `restart_delay` | `float` | `5.0` | Seconds to wait before restarting after a failure |
| `restart_on_failure` | `bool` | `False` | Automatically restart the manager on failure |
| `log_level` | `LogLevel` | `INFO` | PgQueuer log level |
| `mode` | `QueueExecutionMode` | `continuous` | `continuous` (run forever) or `drain` (exit when empty) |
| `max_concurrent_tasks` | `int \| None` | `None` | Global cap on concurrent tasks (`None` = unlimited) |
| `shutdown_on_listener_failure` | `bool` | `False` | Shut down if the NOTIFY listener fails |
| `shutdown` | `asyncio.Event \| None` | `None` | External shutdown event (`None` creates a new one) |

---

## Comparison with the CLI

| | `pgq run` CLI | `await pgqueuer.run()` |
|---|---|---|
| **Invocation** | Shell command | Python function call |
| **Factory** | String only (`module:func`) | String or callable |
| **Shutdown control** | Signals only (SIGINT/SIGTERM) | Signals + `asyncio.Event` |
| **Logging setup** | Automatic | Automatic |
| **Signal handling** | Automatic | Automatic |
| **Restart on failure** | `--restart-on-failure` flag | `restart_on_failure=True` |

Both provide identical runtime behavior: signal handling, supervisor loop, graceful
shutdown, and restart-on-failure support.

---

## See Also

- [CLI Reference](../reference/cli.md) -- full `pgq run` documentation
- [Deployment Patterns](deployment.md) -- horizontal scaling and production deployment
- [Core Concepts](../getting-started/core-concepts.md) -- how PgQueuer works
