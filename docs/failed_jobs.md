# Failed Job Re-queueing

PGQueuer supports marking failed jobs for manual intervention instead of automatically moving them to the log table. This allows administrators to review, fix, and re-queue jobs as needed.

## Overview

By default, when a job fails with an exception, it's moved from the queue table to the log table with an "exception" status. With the failed job feature, you can configure entrypoints to mark failed jobs as "failed" instead, keeping them in the queue table for manual intervention.

## Configuration

Add the `mark_as_failed=True` parameter to your entrypoint decorator:

```python
@qm.entrypoint("data_processing", mark_as_failed=True)
async def process_data(job: Job) -> None:
    # If this job fails, it will be marked as 'failed' 
    # instead of being moved to the log table
    process_important_data(job.payload)
```

## Job Status Flow

### Default Behavior (mark_as_failed=False)
```
queued → picked → exception (moved to log table)
```

### With mark_as_failed=True
```
queued → picked → failed (remains in queue table for manual intervention)
```

## CLI Commands

### List Failed Jobs
```bash
pgq list-failed
```
Shows all jobs currently in "failed" status that are available for re-queueing.

### Re-queue Failed Jobs
```bash
pgq requeue-failed --job-ids "1,2,3"
```
Re-queues specific failed jobs back to "queued" status for reprocessing.

### Manually Mark Jobs as Failed
```bash
pgq mark-failed --job-ids "4,5,6"
```
Manually marks jobs as failed (useful for jobs that need manual review before reprocessing).

## Programmatic API

### Mark Jobs as Failed
```python
await queries.mark_jobs_as_failed([job_id1, job_id2])
```

### Re-queue Failed Jobs
```python
requeued_ids = await queries.requeue_failed_jobs([job_id1, job_id2])
print(f"Re-queued {len(requeued_ids)} jobs")
```

### List Failed Jobs
```python
failed_jobs = await queries.list_failed_jobs()
for job in failed_jobs:
    print(f"Failed job {job.id}: {job.entrypoint}")
```

## Use Cases

### Critical Jobs That Need Manual Review
```python
@qm.entrypoint("payment_processing", mark_as_failed=True)
async def process_payment(job: Job) -> None:
    # Payment failures need manual review before retry
    process_payment_data(job.payload)
```

### Data Processing with External Dependencies
```python
@qm.entrypoint("external_api_sync", mark_as_failed=True, retry_timer=timedelta(minutes=30))
async def sync_with_external_api(job: Job) -> None:
    # API failures might need configuration changes before retry
    sync_data_with_api(job.payload)
```

### Batch Processing Jobs
```python
@qm.entrypoint("large_batch_processing", mark_as_failed=True)
async def process_large_batch(job: Job) -> None:
    # Large batch failures might need resource adjustments
    process_batch_data(job.payload)
```

## Limitations

**Traceback Storage**: Failed jobs don't store traceback information in the database (only in application logs). This is because the queue table doesn't have a traceback column. When debugging failed jobs, check the application logs for exception details.

## Best Practices

1. **Use mark_as_failed for jobs that require human intervention**: Don't use it for jobs that should be automatically retried.

2. **Monitor failed jobs regularly**: Set up alerts or regular checks for jobs in failed status.

3. **Check application logs for exception details**: Since traceback is not stored in the database for failed jobs, use application logs to diagnose failures.

4. **Document failure reasons**: Consider adding context to job payloads to help with manual review.

5. **Set appropriate retry_timer values**: Failed jobs won't be automatically retried, so the retry_timer only affects how long jobs stay "picked" before being eligible for manual intervention.

6. **Clean up old failed jobs**: Consider periodically reviewing and cleaning up failed jobs that are no longer relevant.

## Example Workflow

1. **Job fails and is marked as failed**:
   ```bash
   pgq list-failed
   # Shows: Job ID 123, entrypoint: payment_processing, error in logs
   ```

2. **Administrator reviews the failure**:
   - Check application logs for the exception details
   - Verify external dependencies are working
   - Fix any configuration issues

3. **Re-queue the job**:
   ```bash
   pgq requeue-failed --job-ids "123"
   ```

4. **Job is reprocessed**: The job returns to "queued" status and will be picked up by workers.

## Database Schema

The failed job feature adds a new "failed" status to the existing job status enum. No additional tables are required.

```sql
-- Job statuses including the new 'failed' status
CREATE TYPE pgqueuer_status AS ENUM (
    'queued', 'picked', 'successful', 'exception', 'canceled', 'deleted', 'failed'
);
```

Jobs with "failed" status remain in the main queue table (`pgqueuer`) rather than being moved to the log table (`pgqueuer_log`).