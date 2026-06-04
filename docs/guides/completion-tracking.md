# Completion Tracking

`CompletionWatcher` lets you **await** the final status of a job using PostgreSQL
`LISTEN/NOTIFY` instead of polling.

## Parameters

| Parameter | Type | Default | Purpose |
|-----------|------|---------|---------|
| `refresh_interval` | `timedelta \| None` | **5 s** | Safety-net poll in case a `NOTIFY` was lost. Pass `None` to disable polling and rely solely on notifications |
| `debounce` | `timedelta` | **50 ms** | Coalesces bursts of `NOTIFY`s to reduce query load |

## Basic Usage

```python
from pgqueuer.core.completion import CompletionWatcher
from pgqueuer.queries import Queries

queries = Queries(driver)

async with CompletionWatcher(driver, queries=queries) as watcher:
    status = await watcher.wait_for(job_id)
    # status: "successful", "exception", "canceled", or "deleted"
```

### Completion Watcher State Flow

The watcher monitors a job's progression until it reaches a **terminal state**:

```
                  ┌────────┐
                  │ queued │
                  └───┬──┬─┘
               claim  │  │  delete
                      ▼  ▼
               ┌────────┐ ┌─────────┐
               │ picked │ │ deleted │
               └┬─┬──┬─┬┘ └─────────┘
                │ │  │ │
     complete   │ │  │ │  cancel
                │ │  │ │
                ▼ │  │ ▼
  ┌────────────┐  │  │  ┌───────────┐
  │ successful │  │  │  │ canceled  │
  └────────────┘  │  │  └───────────┘
            error │  │ hold
                  ▼  ▼
        ┌───────────┐ ┌────────┐
        │ exception │ │ failed │
        └───────────┘ └────────┘
```

!!! note "`failed` is not a terminal state for the watcher"
    A held job (`on_failure="hold"`) lands in `failed`, but `wait_for` resolves
    only on `successful`, `exception`, `canceled`, or `deleted`. A held job stays
    unresolved until it is re-queued and reaches one of those states.

## Tracking Many Jobs at Once

```python
from asyncio import gather
from pgqueuer.core.completion import CompletionWatcher

image_ids   = await qm.queries.enqueue(["render_img"]   * 20, [b"..."] * 20, [0] * 20)
report_ids  = await qm.queries.enqueue(["generate_pdf"] * 10, [b"..."] * 10, [0] * 10)
cleanup_ids = await qm.queries.enqueue(["cleanup"]      *  5, [b"..."] *  5, [0] *  5)

async with CompletionWatcher(driver, queries=queries) as w:
    img_statuses, pdf_statuses, clean_statuses = await gather(
        gather(*[w.wait_for(j) for j in image_ids]),
        gather(*[w.wait_for(j) for j in report_ids]),
        gather(*[w.wait_for(j) for j in cleanup_ids]),
    )
```

Terminal states: `canceled`, `deleted`, `exception`, `successful`.

## Helper Patterns

Below are two ready-to-use patterns you can copy into your own code for common
completion-tracking scenarios.

### Wait for all jobs

Block until every supplied job finishes; return statuses in the same order as the input IDs.

```python
import asyncio
from datetime import timedelta
from pgqueuer import db, models
from pgqueuer.core.completion import CompletionWatcher
from pgqueuer.queries import Queries


async def wait_for_all(
    driver: db.Driver,
    job_ids: list[models.JobId],
    refresh_interval: timedelta = timedelta(seconds=5),
    debounce: timedelta = timedelta(milliseconds=50),
) -> list[models.JOB_STATUS]:
    async with CompletionWatcher(
        driver,
        queries=Queries(driver),
        refresh_interval=refresh_interval,
        debounce=debounce,
    ) as watcher:
        waiters = [watcher.wait_for(jid) for jid in job_ids]
        return await asyncio.gather(*waiters)
```

### Wait for first job

Return as soon as **any** job hits a terminal state; cancel pending waiters.

```python
async def wait_for_first(
    driver: db.Driver,
    job_ids: list[models.JobId],
    refresh_interval: timedelta = timedelta(seconds=5),
    debounce: timedelta = timedelta(milliseconds=50),
) -> models.JOB_STATUS:
    async with CompletionWatcher(
        driver,
        queries=Queries(driver),
        refresh_interval=refresh_interval,
        debounce=debounce,
    ) as watcher:
        waiters = [watcher.wait_for(jid) for jid in job_ids]
        done, pending = await asyncio.wait(
            waiters, return_when=asyncio.FIRST_COMPLETED
        )
        for fut in pending:
            fut.cancel()

    return next(iter(done)).result()
```

## Notification Reliability

To improve reliability without heavy polling:

1. **Listener health check** — Run with the `pgq run --shutdown-on-listener-failure` flag
   (or pass `shutdown_on_listener_failure=True` to `QueueManager.run()`) so the manager stops
   (and can be restarted by a supervisor) if the LISTEN channel becomes unhealthy.

2. **Minimize the refresh poll** — Set a long `refresh_interval` to rely primarily on
   notifications when your channel is stable:

    ```python
    async with CompletionWatcher(driver, queries=Queries(driver), refresh_interval=timedelta(minutes=5)) as w:
        status = await w.wait_for(job_id)
    ```
