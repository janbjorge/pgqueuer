# Connection failures

What a worker does when its database connection drops. Code references are to
the commit that added this page. Numbers come from killing the worker's backends
with `pg_terminate_backend` (10 jobs, `restart_delay` 1 s, `dequeue_timeout`
30 s, `heartbeat_timeout` 4 s).

## How the parts depend on each other

Each row lists what the part calls and what happens when that call fails on a
dead connection. The first tree is ownership. The second is how NOTIFYs flow
back from the connection.

```
supervisor                                                         reruns factory (opt-in)   supervisor.py:86
└─ factory() → PgQueuer                                            new connection per cycle
   └─ PgQueuer.run                                                 first error stops both    applications.py:156
      ├─ QueueManager.run                                                                    qm.py:390
      │  ├─ main loop                ─► dequeue + probes           raises → run ends         qm.py:466
      │  ├─ job task (per job)                                     waited for                tm.py:78
      │  │  ├─ heartbeats, buffered  ─► update_heartbeat           logged, kept              buffers.py:71
      │  │  └─ results, buffered     ─► log_jobs                   exit: 5 tries, dropped    buffers.py:82
      │  ├─ health check, 10 s       ─► notify                     ends run (opt-in)         qm.py:113
      │  └─ log aggregation          ─► aggregate_logs             swallowed                 qm.py:126
      └─ SchedulerManager.run                                                                sm.py:92
         ├─ loop, cron + 1 s         ─► fetch_schedule             raises → run ends         sm.py:117
         ├─ dispatch                 ─► set_schedule_queued        logged                    sm.py:179
         └─ heartbeat loop, 1 s      ─► update_schedule_heartbeat  logged                    sm.py:189

Every ─► runs Queries → Driver → connection. Nothing on that path retries.

connection
└─ driver listener      asyncpg callback / psycopg watcher  psycopg.py:109
   └─ EventRouter                                           listeners.py:24
      ├─ table changed  wakes the main loop                 listeners.py:53
      ├─ health check   resolves the health check           listeners.py:61
      └─ cancellation   cancels the job                     listeners.py:56
```

- **Nothing reconnects in place.** `Queries` and the drivers pass every error
  straight up (`queries.py:264-268`, `drivers/asyncpg.py:52-66`,
  `drivers/psycopg.py:62-83`). The one exception is asyncpg's pool, which
  replaces its own query connections.
- **A run ends at the first "raises → run ends".** How soon that happens
  depends on when each loop next wakes: the main loop on NOTIFY or
  `dequeue_timeout` (`qm.py:496-505`), and the scheduler at the next cron
  + 1 s, or every 1 s with no schedules (`sm.py:136-147`).
- **psycopg fails fast.** Its watcher sits in `notifies()` (`psycopg.py:115`),
  so it dies at once and the next query fails.
- **The health check only acts with `shutdown_on_listener_failure`.** Its
  failure then wakes the main loop at once (`qm.py:493-505`) and ends the run
  (`qm.py:355-369`).
- **An ended run stays ended** unless `pgq run --restart-on-failure` or
  `pgqueuer.run(restart_on_failure=True)` reruns the factory
  (`supervisor.py:86-117`). Both flags default to off (`cli.py:567-591`,
  `supervisor.py:128-129`).

## Measured, with restart on

| Driver | Setup | Restart after kill | New jobs |
|---|---|---|---|
| asyncpg connection | no schedules | 1 s (all of it `restart_delay`) | all |
| asyncpg connection | one schedule | 30 s (the next `dequeue_timeout` wake) | all, after restart |
| asyncpg connection | one schedule, with `shutdown_on_listener_failure` | 8 s (30 s before this change) | all, after restart |
| psycopg connection | any | 1 s | all |
| asyncpg pool | default | never | only by 30 s polling; NOTIFY is gone for good |
| asyncpg pool | with `shutdown_on_listener_failure` | 18 s (30 s before this change) | all |

With restart off, both connection drivers exit with an error, and the pool keeps
running in the degraded state.

## Problems

1. **In-flight jobs run twice.** On failure the queue manager waits for running
   jobs (`tm.py:78-79`). Their results sit in `JobStatusLogBuffer`, whose final
   flush runs on the dead connection. It tries 5 times and then drops them
   (`buffers.py:82-97`). The rows stay `picked`, so they are picked again after
   `heartbeat_timeout`. Measured: 10/10 in-flight jobs ran twice, on both
   asyncpg and psycopg. With psycopg this also happened once to jobs that had
   finished before the kill. INFERRED: their flush was still waiting for the
   connection lock held by `notifies()`.
2. **How fast a drop is noticed depends on unrelated settings.** Without
   `shutdown_on_listener_failure` it takes 1 s or 30 s, depending on whether
   a schedule exists and on `dequeue_timeout`. With the flag it is at most
   about 20 s: up to 10 s until the next probe, plus its 10 s timeout.
3. **The pool hides the failure.** The LISTEN connection is held out of the
   pool (`drivers/asyncpg.py:152`) and dies silently. Queries move to fresh
   pool connections, so nothing raises and nothing restarts.

## What other queues do

River, Oban, GoodJob, graphile-worker, pg-boss and Procrastinate give the
listener its own connection. They ping it every 5-10 s, reconnect it in place
with backoff, and always keep polling every 1-10 s, so losing NOTIFY costs only
latency ([River](https://github.com/riverqueue/river/blob/master/internal/notifier/notifier.go),
[GoodJob](https://github.com/bensheldon/good_job/blob/main/lib/good_job/notifier.rb),
[graphile-worker](https://github.com/graphile/worker/blob/main/src/main.ts)).
[Que](https://github.com/que-rb/que/blob/master/lib/que/locker.rb) lets the
process die and relies on a supervisor. PgQueuer today works like Que.

## Decision

Keep the current model. The user passes a connection, and recovery means the
supervisor reruns the factory. PgQueuer does not reconnect in place, and does
not take a driver factory. Follow-ups, one small PR each:

1. **Notice a dead listener fast.** Done: a failed health check now wakes the
   main loop (`qm.py:493-505`) instead of waiting for the next
   `dequeue_timeout`.
2. **Results lost at restart.** Open: document it as at-least-once on
   connection loss, or hand unflushed results to the next cycle.
3. **Defaults.** Turn on `restart_on_failure` and
   `shutdown_on_listener_failure`. This changes behaviour, so it waits for
   v1.7.
4. **Pool drivers.** Deprecate them (#951).
