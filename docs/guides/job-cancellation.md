# Job Cancellation

PgQueuer supports canceling queued or in-progress jobs programmatically via PostgreSQL's
NOTIFY system. Cancellations are best-effort and may not halt a job already underway.

## Enqueueing jobs

```python
from pgqueuer.queries import Queries

queries = Queries(db_driver)
job_ids = await queries.enqueue("task_entrypoint", b"Job data", priority=5)
```

From synchronous code:

```python
from pgqueuer.queries import SyncQueries

queries = SyncQueries(sync_db_driver)
job_ids = queries.enqueue("task_entrypoint", b"Job data", priority=5)
```

## Cancelling jobs

```python
await queries.mark_job_as_cancelled(job_ids)
```

`mark_job_as_cancelled` accepts a list of job IDs. A job that is still `queued` is
removed at once and logged as `canceled`. A job that is already `picked` stays in the
queue until its worker logs the outcome, so it keeps its `concurrency_limit` slot and
its `dedupe_key` while the handler runs. The NOTIFY still asks the handler to stop.
Jobs that have already completed are unaffected.

## Handling cancellation in job code

Use the job's cancellation scope to stop processing when a cancel request arrives:

Declare a `Context` parameter and PgQueuer injects the job's cancellation scope:

```python
from pgqueuer.models import Context, Job

@pgq.entrypoint("task_entrypoint")
async def process_job(job: Job, ctx: Context) -> None:
    with ctx.cancellation:
        await perform_task(job.payload)
```

## Job status after cancellation

A queued job is logged `canceled` immediately. A picked job is logged `canceled` when
its worker sees the request and the handler returns; until that log, the row stays
`picked`. See [Architecture](../reference/architecture.md) for the full status lifecycle.
