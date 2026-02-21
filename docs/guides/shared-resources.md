# Shared Resources (`Context.resources`)

PgQueuer lets you provide a single shared resources container that is injected into every job
execution context. This makes it easy to initialize heavyweight or shared components (database
pools, HTTP clients, caches, ML models, etc.) once at process startup and reuse them across
all jobs.

## Why Use Shared Resources?

- Avoid re-initializing expensive objects per job (e.g. HTTP session pools, model weights)
- Centralize lifecycle (create at startup, optionally close at shutdown)
- Enable coordinated state (e.g. in‑memory counters, feature flags)
- Provide a structured place for integrations (tracing, metrics, external APIs)

## Providing Resources

You pass a mutable mapping when constructing `PgQueuer` (or `QueueManager` directly):

```python
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job

async def build_pgqueuer() -> PgQueuer:
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)

    resources = {
        "http_client": build_http_client(),   # e.g. httpx.AsyncClient()
        "vector_index": load_vector_index(),  # custom object
        "feature_flags": {"beta_mode": True},
    }

    pgq = PgQueuer(driver, resources=resources)

    @pgq.entrypoint("process_user")
    async def process_user(job: Job) -> None:
        ctx = pgq.qm.get_context(job.id)
        http = ctx.resources["http_client"]
        flags = ctx.resources["feature_flags"]
        # Use shared objects without recreating them
        ...

    return pgq
```

Internally this mapping is passed into each `Context` as `context.resources`. All jobs receive
the **same object** (it is not copied), so mutations are visible across jobs.

## Access Inside Custom Executors

If you implement a custom executor (`AbstractEntrypointExecutor`), the `execute(self, job, context)`
method receives the `Context`:

```python
from pgqueuer.executors import AbstractEntrypointExecutor
from pgqueuer.models import Job, Context

class LoggingExecutor(AbstractEntrypointExecutor):
    async def execute(self, job: Job, context: Context) -> None:
        logger = context.resources.get("logger")
        if logger:
            logger.info("Processing job %s", job.id)
        # Call wrapped function (if delegating) or implement logic directly
```

## Mutating Resources

Because `resources` is a shared mutable mapping:

```python
context.resources.setdefault("metrics", {}).setdefault("processed", 0)
context.resources["metrics"]["processed"] += 1
```

If you need stricter control (immutability, lifecycle hooks), you can replace the mapping with
a custom registry class; the public contract is simply "object with mapping semantics."

## Enabling Context Injection

Entry points only receive the runtime `Context` when registered with `accepts_context=True`.

```python
@pgq.entrypoint("process_with_context", accepts_context=True)
async def process_with_context(job: Job, ctx: Context) -> None:
    logger = ctx.resources.get("logger")
    ...
```

Entry points registered without the flag are invoked with the job only:

```python
@pgq.entrypoint("process_without_context")
async def process_without_context(job: Job) -> None:
    ...
```

## Scheduled Tasks

Currently, scheduled tasks do **not** automatically receive `resources` as a second argument.
Access them via closure:

```python
pgq = PgQueuer(driver, resources={"http": http_client})

@pgq.schedule("refresh_cache", "*/5 * * * *")
async def refresh_cache(schedule):
    http = pgq.resources["http"]
    await http.get("https://api.example.com/ping")
```

## Using Async Resources in Sync Entrypoints

Sync entrypoints (declared with `def`, not `async def`) run in a worker thread via
`anyio.to_thread.run_sync`. Async resources **must not** be called directly inside sync
functions.

**Recommended approaches:**

1. Prefer making I/O entrypoints async:

    ```python
    @pgq.entrypoint("process_user")
    async def process_user(job: Job) -> None:
        pool = pgq.qm.get_context(job.id).resources["pg_pool"]
        await pool.execute("SELECT 1")
    ```

2. If you must stay sync but still need async calls, bridge them with `anyio.from_thread.run`:

    ```python
    @pgq.entrypoint("resize_then_store")
    def resize_then_store(job: Job) -> None:
        ctx = pgq.qm.get_context(job.id)
        img = cpu_resize(job.payload)
        anyio.from_thread.run(store_image, img, ctx.resources["pg_pool"])

    async def store_image(data: bytes, pool: asyncpg.Pool) -> None:
        await pool.execute("INSERT INTO images(data) VALUES($1)", data)
    ```

!!! warning "Do not call async resources directly in sync entrypoints"
    ```python
    @pgq.entrypoint("bad")
    def bad(job: Job) -> None:
        async_func = ctx.resources["async_func"]
        coro = async_func("value")  # Returns coroutine, never awaited!
    ```

## Testing With Resources

```python
qm = QueueManager(driver, resources={"flag": "test"})

@qm.entrypoint("demo")
async def demo(job: Job) -> None:
    assert qm.get_context(job.id).resources["flag"] == "test"
```

## Summary

| Aspect | Behavior |
|--------|----------|
| Initialization | Passed at construction: `PgQueuer(..., resources=...)` |
| Scope | Shared across all jobs in the same process |
| Mutation | Visible to subsequent jobs |
| Scheduled jobs | Use closure access (for now) |
| Custom executors | Receive via `context.resources` |
