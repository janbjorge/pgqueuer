# Heartbeat Monitoring

PgQueuer's automatic heartbeat mechanism ensures active jobs are continuously monitored
for liveness.

## How It Works

While a job is in the `picked` state, the `QueueManager` periodically updates a `heartbeat`
timestamp on the job record. This signals that the job is still actively being processed.

- **Periodic updates**: The heartbeat timestamp is refreshed at a configurable interval.
- **Stall detection**: External monitoring can compare `heartbeat` against `NOW()` to
  identify stalled or hung jobs.
- **Resource management**: Prevents unresponsive jobs from holding locks indefinitely,
  enabling external supervisors to detect and handle stuck workers.

## Stall Detection Pattern

You can query for stalled jobs directly in PostgreSQL:

```sql
-- Jobs that haven't updated their heartbeat in the last 5 minutes
SELECT id, entrypoint, status, heartbeat
FROM pgqueuer
WHERE status = 'picked'
  AND heartbeat < NOW() - INTERVAL '5 minutes';
```

## Heartbeat Timeout

The `heartbeat_timeout` parameter on `pgq.run()` / `QueueManager.run()` sets the
duration after which a picked job with a stale heartbeat becomes eligible for
re-pickup by any available worker. Heartbeats are sent automatically at half
this interval. This enables automatic recovery from crashed or stalled workers:

```python
from datetime import timedelta

await pgq.run(
    heartbeat_timeout=timedelta(minutes=5),
)
```

With `heartbeat_timeout` set, a job that stops updating its heartbeat for the
specified duration will be retried by the next available worker.

!!! note
    The default `heartbeat_timeout` is 30 seconds. Set it to match your expected
    maximum job runtime plus a safety margin to avoid prematurely re-queuing
    legitimately long-running jobs.
