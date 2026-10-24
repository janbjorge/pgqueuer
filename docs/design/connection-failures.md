# Connection failures

What a worker does when its database connection drops. It is a sub-model of
the [system design](README.md). Running a job twice after a drop is allowed by
[ADR-0004](../adr/ADR-0004-delivery-is-at-least-once.md), and the user-facing
summary is in the [deployment guide](../guides/deployment.md#connection-loss).

## How the parts depend on each other

The first tree shows who owns what, which query each part runs, and what
happens when that query hits a dead connection. The second shows how NOTIFYs
come back.

```
supervisor                                                         reruns the factory (opt-in)
└─ factory() → PgQueuer                                            new connection per cycle
   └─ PgQueuer.run                                                 first error stops both managers
      ├─ QueueManager.run
      │  ├─ main loop                ─► dequeue + probes           raises, run ends
      │  ├─ job task (per job)                                     waited for
      │  │  ├─ heartbeats, buffered  ─► update_heartbeat           logged, kept
      │  │  └─ results, buffered     ─► log_jobs                   at exit: 5 tries, then kept
      │  ├─ health check, 10 s       ─► notify                     ends run (opt-in)
      │  └─ log aggregation          ─► aggregate_logs             swallowed
      └─ SchedulerManager.run
         ├─ loop, cron + 1 s         ─► fetch_schedule             raises, run ends
         ├─ dispatch                 ─► set_schedule_queued        logged
         └─ heartbeat loop, 1 s      ─► update_schedule_heartbeat  logged

Every ─► goes through Queries → Driver → connection, with no retry.

connection
└─ driver listener      asyncpg callback, psycopg watcher task
   └─ EventRouter
      ├─ table changed  wakes the main loop
      ├─ health check   answers the health check
      └─ cancellation   cancels the job
```

Nothing reconnects in place. `Queries` and the drivers raise every error to the
caller. The only exception is asyncpg's pool, which replaces its own query
connections.

A run ends at the first "raises, run ends", so how fast a drop is noticed
depends on when each loop next wakes. The main loop wakes on a NOTIFY or after
`dequeue_timeout`. The scheduler wakes at the next cron time plus 1 s, or every
second when no schedules are registered. psycopg is faster than both: its
watcher waits inside `notifies()`, so it fails at once and takes the next query
with it. With `shutdown_on_listener_failure`, a failed health check also wakes
the main loop and ends the run, which bounds detection at about 20 s: up to
10 s until the next probe, plus its 10 s timeout.

An ended run stays ended unless `pgq run --restart-on-failure` or
`pgqueuer.run(restart_on_failure=True)` reruns the factory. Both
`restart_on_failure` and `shutdown_on_listener_failure` default to off.

## Measured

One run on 2026-10-14. The worker's backends were killed with
`pg_terminate_backend`, with 10 jobs, `restart_on_failure` on, `restart_delay`
1 s, `dequeue_timeout` 30 s and `heartbeat_timeout` 4 s.

| Driver | Setup | Restart after the kill | New jobs |
|---|---|---|---|
| asyncpg connection | no schedules | 1 s, all of it `restart_delay` | all |
| asyncpg connection | one schedule | 30 s, at the next `dequeue_timeout` | all, after the restart |
| asyncpg connection | one schedule, `shutdown_on_listener_failure` | 8 s | all, after the restart |
| psycopg connection | any | 1 s | all |
| asyncpg pool | default | never | only by polling every 30 s |
| asyncpg pool | `shutdown_on_listener_failure` | 18 s | all |

With restart off, both connection drivers exit with an error, and the pool keeps
running without NOTIFY.

## Known gaps

Jobs running at the drop still finish. Their results sit in `JobStatusLogBuffer`,
whose last flush runs on the dead connection. It tries five times.
`QueueManager.run` then keeps whatever remains on `unwritten_job_logs`.
`--restart-on-failure` writes those ends on the next cycle's connection before
dequeuing. A caller that handles the error itself can pass the same list to
`log_jobs`. A write after another worker owns the job is ignored. A new process
does not have the list, so those rows stay `picked` and run again after
`heartbeat_timeout`. ADR-0004 allows that second run. In the 2026-10-14 run,
before the ends were kept, all 10 in-flight jobs ran twice on both asyncpg and
psycopg. With psycopg it also happened once to jobs that had finished before the
kill, probably because their flush was still waiting for the connection lock
that `notifies()` holds.

The asyncpg pool hides the failure. `AsyncpgPoolDriver` keeps its LISTEN
connection out of the pool, and that connection dies without an error. Queries
move to fresh pool connections, so nothing raises and nothing restarts. The
[performance guide](../guides/performance-tuning.md#notify-channel-and-polling-fallback)
already advises a direct connection for the queue manager, and #951 deprecates
the pool drivers.

## What other queues do

River, Oban, GoodJob, graphile-worker, pg-boss and Procrastinate give the
listener its own connection. They ping it every 5 to 10 s, reconnect it in place
with backoff, and keep polling every 1 to 10 s, so losing NOTIFY only adds
latency ([River](https://github.com/riverqueue/river/blob/master/internal/notifier/notifier.go),
[GoodJob](https://github.com/bensheldon/good_job/blob/main/lib/good_job/notifier.rb),
[graphile-worker](https://github.com/graphile/worker/blob/main/src/main.ts)).
[Que](https://github.com/que-rb/que/blob/master/lib/que/locker.rb) lets the
process die and relies on a supervisor, and PgQueuer works the same way.

## Decision

PgQueuer keeps its current model: the user passes a connection, and recovery
means the supervisor reruns the factory. PgQueuer does not reconnect in place
and does not take a driver factory. A failed shutdown flush keeps the job ends
in the process, on `QueueManager.unwritten_job_logs`. An in-process restart
writes them; a new process does not have them, and ADR-0004 allows the second
run. Open follow-ups:

1. Turn on `restart_on_failure` and `shutdown_on_listener_failure` by default.
   This changes behaviour, so it waits for v1.7.
2. Deprecate the pool drivers (#951).
