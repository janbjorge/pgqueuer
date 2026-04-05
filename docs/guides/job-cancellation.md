# Job Cancellation

PgQueuer supports canceling queued or in-progress jobs programmatically via PostgreSQL's
NOTIFY system. Cancellations are best-effort and may not halt a job already underway.

## Enqueueing Jobs

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

## Cancelling Jobs

```python
await queries.mark_job_as_cancelled(job_ids)
```

`mark_job_as_cancelled` accepts a list of job IDs. Jobs that have already completed are
unaffected.

## Handling Cancellation in Job Code

Use the job's cancellation scope to stop processing when a cancel request arrives:

```python
@pgq.entrypoint("task_entrypoint")
async def process_job(job: Job) -> None:
    with pgq.qm.get_context(job.id).cancellation:
        await perform_task(job.payload)
```

## Job Status After Cancellation

Cancelled jobs transition to the `canceled` status and are logged in the statistics table.
See [Architecture](../reference/architecture.md) for the full status lifecycle.
