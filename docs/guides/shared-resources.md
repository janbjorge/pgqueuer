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
from contextlib import asynccontextmanager
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Context, Job

@asynccontextmanager
async def build_pgqueuer():
    conn = await asyncpg.connect()
    driver = AsyncpgDriver(conn)

    resources = {
        "http_client": build_http_client(),   # e.g. httpx.AsyncClient()
        "vector_index": load_vector_index(),  # custom object
        "feature_flags": {"beta_mode": True},
    }

    pgq = PgQueuer(driver, resources=resources)

    # Annotating a parameter as Context is enough — PgQueuer injects it.
    @pgq.entrypoint("process_user")
    async def process_user(job: Job, ctx: Context) -> None:
        http = ctx.resources["http_client"]
        flags = ctx.resources["feature_flags"]
        # Use shared objects without recreating them
        ...

    yield pgq
```

Internally this mapping is passed into each `Context` as `context.resources`. All jobs receive
the **same object** (it is not copied), so mutations are visible across jobs.

## Typed Access with `ResourceKey`

Reading `ctx.resources["db"]` works, but the key is a bare string and the value is untyped — a
typo surfaces only at runtime and your type checker can't tell you what came back. `ResourceKey`
is an optional, additive way to get type-checked access without changing how resources are stored:

```python
import asyncpg
from pgqueuer.models import Context, Job, ResourceKey

# Define keys once, near your resource setup.
DB = ResourceKey("db", asyncpg.Pool)

pgq = PgQueuer(driver, resources={DB.name: pool})

@pgq.entrypoint("process_user")
async def process_user(job: Job, ctx: Context) -> None:
    pool = ctx.resource(DB)   # statically typed as asyncpg.Pool
    async with pool.acquire() as conn:
        ...
```

`ctx.resource(key)` returns the value typed as `key.type`, and validates at runtime:

- a missing key raises `KeyError` naming the resource,
- a value of the wrong type raises `TypeError` naming both the actual and expected type.

`ScheduleContext` exposes the same `resource()` method, and `key.resolve(mapping)` is available if
you hold a resources mapping directly. The token is purely opt-in: existing
`ctx.resources["db"]` access continues to work unchanged.

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

Context injection is **auto-detected from the handler signature**. Annotate a parameter as
`Context` and PgQueuer passes the runtime `Context`:

```python
@pgq.entrypoint("process_with_context")
async def process_with_context(job: Job, ctx: Context) -> None:
    logger = ctx.resources.get("logger")
    ...
```

Detection is annotation-driven, so a handler with an unrelated extra parameter
(`async def f(job: Job, config: dict | None = None)`) is left untouched—no `Context` is injected
into it. Entry points that declare only the job are invoked with the job alone:

```python
@pgq.entrypoint("process_without_context")
async def process_without_context(job: Job) -> None:
    ...
```

If you need to override the detection (for example, a wrapped callable whose signature does not
reflect how it is invoked), pass `accepts_context=True` or `accepts_context=False` explicitly:

```python
@pgq.entrypoint("forced", accepts_context=True)
async def forced(job: Job, ctx: Context) -> None:
    ...
```

## Scheduled Tasks

Scheduled tasks follow the same rule: annotate a parameter as `ScheduleContext` and it is
injected automatically.

```python
from pgqueuer.models import Schedule, ScheduleContext

@pgq.schedule("refresh_cache", "*/5 * * * *")
async def refresh_cache(schedule: Schedule, ctx: ScheduleContext) -> None:
    http = ctx.resources["http"]
    await http.get("https://api.example.com/ping")
```

Tasks that declare only the schedule argument continue to work unchanged:

```python
@pgq.schedule("simple_task", "*/5 * * * *")
async def simple_task(schedule: Schedule) -> None:
    await perform_task()
```

## Testing With Resources

```python
from pgqueuer.queries import Queries

qm = QueueManager(Queries(driver), resources={"flag": "test"})

@qm.entrypoint("demo")
async def demo(job: Job, ctx: Context) -> None:
    assert ctx.resources["flag"] == "test"
```

## Summary

| Aspect | Behavior |
|--------|----------|
| Initialization | Passed at construction: `PgQueuer(..., resources=...)` |
| Scope | Shared across all jobs in the same process |
| Mutation | Visible to subsequent jobs |
| Typed access | `ctx.resource(ResourceKey("db", Pool))` — type-checked, runtime-validated |
| Context injection | Auto-detected from a `Context`-annotated parameter; override with `accepts_context` |
| Scheduled jobs | Annotate a `ScheduleContext` parameter to receive resources |
| Custom executors | Receive via `context.resources` |
