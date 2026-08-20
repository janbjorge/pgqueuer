# Completion Tracking

`CompletionWatcher` lets you **await** the final status of a job using PostgreSQL
`LISTEN/NOTIFY` instead of polling.

## Parameters

| Parameter | Type | Default | Purpose |
|-----------|------|---------|---------|
| `refresh_interval` | `timedelta \| None` | **5 s** | Safety-net poll in case a `NOTIFY` was lost. Pass `None` to disable polling and rely solely on notifications |
| `debounce` | `timedelta` | **50 ms** | Coalesces bursts of `NOTIFY`s to reduce query load |

## Basic usage

```python
from pgqueuer.core.completion import CompletionWatcher
from pgqueuer.queries import Queries

queries = Queries(driver)

async with CompletionWatcher(driver, queries=queries) as watcher:
    status = await watcher.wait_for(job_id)
    # status: "successful", "exception", "canceled", or "deleted"
```

### Completion watcher state flow

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

## Tracking many jobs at once

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

!!! note "Batches enqueued with `on_conflict=\"skip\"`"
    Skipped duplicates come back as `None` instead of a job id. Filter them out before
    waiting: `[w.wait_for(j) for j in job_ids if j is not None]`.

Terminal states: `canceled`, `deleted`, `exception`, `successful`.

## Helper patterns

Two patterns to copy into your own code.

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

## Notification reliability

Two settings keep the watcher reliable without leaning on the refresh poll.

Run with `pgq run --shutdown-on-listener-failure` (or pass
`shutdown_on_listener_failure=True` to `QueueManager.run()`) so the manager stops when the
LISTEN channel goes unhealthy and a supervisor can restart it.

Then, if the channel is stable, raise `refresh_interval` so notifications carry the traffic
and the poll stays a fallback:

```python
async with CompletionWatcher(driver, queries=Queries(driver), refresh_interval=timedelta(minutes=5)) as w:
    status = await w.wait_for(job_id)
```
