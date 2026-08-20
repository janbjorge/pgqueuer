# Heartbeat Monitoring

PgQueuer updates a heartbeat timestamp on every active job so that stalled or
crashed workers can be detected.

## How it works

While a job is in the `picked` state, the `QueueManager` refreshes a `heartbeat`
timestamp on the job row at a configurable interval. A timestamp that stops moving is
the signal that the worker holding the job died or hung.

Anything with database access can act on that: PgQueuer itself re-picks jobs whose
heartbeat is older than `heartbeat_timeout`, and your own monitoring can compare
`heartbeat` against `NOW()` to alert on stuck workers.

## Stall detection pattern

You can query for stalled jobs directly in PostgreSQL:

```sql
-- Jobs that haven't updated their heartbeat in the last 5 minutes
SELECT id, entrypoint, status, heartbeat
FROM pgqueuer
WHERE status = 'picked'
  AND heartbeat < NOW() - INTERVAL '5 minutes';
```

## Heartbeat timeout

The `heartbeat_timeout` parameter on `pgq.run()` / `QueueManager.run()` sets the
duration after which a picked job with a stale heartbeat becomes eligible for
re-pickup by any available worker. Heartbeats are sent automatically at half
this interval, so a crashed or stalled worker's jobs recover without operator
action:

```python
from datetime import timedelta

await pgq.run(
    heartbeat_timeout=timedelta(minutes=5),
)
```

With `heartbeat_timeout` set, a job that stops updating its heartbeat for the
specified duration will be retried by the next available worker.

Workers started from the command line configure the same setting with
`--heartbeat-timeout` (in seconds):

```bash
pgq run my_module:my_factory --heartbeat-timeout 300
```

!!! note
    The default `heartbeat_timeout` is 30 seconds. Set it to match your expected
    maximum job runtime plus a safety margin to avoid prematurely re-queuing
    legitimately long-running jobs.
