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

## How the cadence is chosen

Each running job queues a heartbeat every `heartbeat_timeout / 2` (b), and the
worker writes the queued heartbeats in one `UPDATE` every `heartbeat_timeout / 8`
(f, with ±20% jitter). A dequeue can re-pick a live job only once its stored
heartbeat is older than `heartbeat_timeout` (T). That age can approach

```
b + 1.2 * (r + 1) * f + X
```

where r is the number of heartbeat writes in a row that fail (a failed write puts
the heartbeats back for the next flush) and X is latency: dispatch delay, the
previous write's duration, the duration of each failed write, network time,
waiting for the row lock and event-loop lag. No smaller bound holds: with no
failures the real code comes within a few milliseconds of it.

The values meet two requirements:

- One failed heartbeat write must not let a live job be re-picked, with 0.2 T
  still left for latency, the failed write included: `b + 2.4 f <= 0.8 T`.
  Nothing bounds a heartbeat write's duration today (no `statement_timeout`),
  so a write that hangs can still use up that 0.2 T.
- Each running job writes its row every T/2, because those row writes are what
  heartbeats cost.

With b = T/2 the first requirement gives f <= T/8.
`test_cadence_keeps_a_live_job_through_one_failed_flush` enforces it. A change to
either value has to argue against one of these requirements.

| Cadence | Oldest heartbeat, no failures | After one failed write |
|---|---|---|
| b = T or T/2, f = T/2 or T (ratios of #171, #333, #368 in today's code) | 1.6 T to 1.7 T | 2.2 T to 2.9 T |
| b = T/2, f = T/4 (#431) | 0.8 T | 1.1 T |
| b = T/2, f = T/8 (current) | 0.65 T | 0.8 T |

Every cadence before #431 let a live job outlast T even without latency, which
is what #323 and #430 reported.

Latency does not scale with T. The psycopg driver's listener can hold the
connection for about a second at a time, adding up to about 2 s. That needs T of
10 s or more when failed writes are fast, and about 15 s if a failed write also
waits on that connection. Use the same `heartbeat_timeout` on every worker: a
worker with a longer timeout heartbeats too slowly for one with a shorter
timeout.
