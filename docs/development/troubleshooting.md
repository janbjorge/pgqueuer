# Driver Troubleshooting

Work through this checklist before you go reading PgQueuer internals. It covers the rough
edges people actually hit in `asyncpg` and `psycopg`, and the questions that separate a
configuration problem from a bug.

## 1. Connection basics

- **Autocommit drift**: psycopg defaults to transactional mode while asyncpg autocommits.
  Verify `connection.autocommit` (or pool init hooks) so enqueued jobs become visible
  immediately.
- **Pools returning dirty state**: Reused connections may keep open transactions or altered
  settings. Confirm pool size fits workload and that teardown callbacks reset state.
- **Authentication/network churn**: Sudden bursts can hit `max_connections`, stale
  certificates, or mismatched DSNs. Cross-check the exact DSN with `psql`, check
  `pg_hba.conf`, and ensure SSL parameters match the server.

## 2. Query flow and transactions

- **Locked rows**: Long-lived transactions block `SELECT … FOR UPDATE SKIP LOCKED`. Inspect
  `pg_locks` + `pg_stat_activity` for blockers and keep DDL or maintenance outside hot paths.
- **Unexpected timeouts**: Server-side `statement_timeout` or driver-level timeouts cancel
  dequeues. List active timeout settings (asyncpg `timeout`, psycopg `options`) and compare
  with production defaults.
- **Type adapters**: Binary payloads must match table encoding. Confirm `pgqueuer` table
  column types and ensure custom adapters/serializers are registered before enqueueing.

## 3. Driver quirks

**asyncpg:**

- Cancels running queries when a task is cancelled; requires manual retry after
  `ConnectionDoesNotExist`.
- Double-check `transaction()` contexts are exited cleanly.
- Surface `PostgresError` details in logs.

**psycopg (sync/async):**

- Mixing `%s` and `$1` placeholders breaks prepared statement caching.
- Async connections must be `await conn.close()`; sync connections should stay out of asyncio
  event loops.
- **Windows event loop**: async psycopg rejects the default `ProactorEventLoop`. Use
  `SelectorEventLoop` (e.g. `asyncio.run(main, loop_factory=asyncio.SelectorEventLoop)` on
  3.12+). The `pgq` CLI does this for you; embedded use must opt in. See
  [drivers reference](../reference/drivers.md#psycopgdriver) for full snippets.

## 4. PgQueuer expectations

- Health checks may raise exceptions from `pgqueuer.errors` (e.g. `FailingListenerError`);
  capture them during service startup.
- LISTEN/NOTIFY keeps consumers awake. Firewalls that drop idle sockets or missing
  `pg_notify` privileges starve queues; monitor `pg_notification_queue_usage()`.
- Payloads are stored as `bytea`; producers and consumers must agree on encoding.

## 5. Rapid triage questions

1. Can a fresh `psql` session connect with the same DSN?
2. Are autocommit and transaction status clean before returning pooled connections?
3. What do `pg_locks` / `pg_stat_activity` report for the queue tables?
4. Are LISTEN/NOTIFY messages flowing end-to-end?
5. Did timeouts or pool recycling settings change recently?
6. Are payloads serialized consistently across services?
7. Did a driver upgrade land without matching PgQueuer expectations?

Write the answers down alongside the relevant logs before escalating. Most reports that
reach the issue tracker turn out to be one of the items above.
