# Performance Tuning

This page covers the knobs available for tuning PgQueuer throughput and latency in production.

## Batch Size

`batch_size` controls how many jobs are fetched from the database in a single `FOR UPDATE SKIP LOCKED` query.

```python
await pgq.run(batch_size=25, dequeue_timeout=timedelta(seconds=10))
```

| Parameter | Default | Effect |
|-----------|---------|--------|
| `batch_size` | `10` | Jobs fetched per dequeue round-trip |
| `dequeue_timeout` | `30 s` | How long to wait for new NOTIFY before polling |

**Tuning guidance:**
- Increase `batch_size` when jobs are short-lived (< 1 s) and you see many dequeue round-trips
  per second in your query logs.
- Decrease it when jobs are long-running and you want finer concurrency control.
- `max_concurrent_tasks` must be at least `2 × batch_size`; PgQueuer enforces this.

## Concurrency Limits

Limit concurrent execution globally or per-entrypoint:

```python
# Global cap across all entrypoints
await pgq.run(batch_size=20, max_concurrent_tasks=100)

# Per-entrypoint cap
@pgq.entrypoint("resize_image", concurrency_limit=4)
async def resize_image(job: Job) -> None:
    ...

# Per-entrypoint rate cap
@pgq.entrypoint("send_sms", requests_per_second=10.0)
async def send_sms(job: Job) -> None:
    ...
```

Use per-entrypoint limits when specific tasks share a scarce external resource (e.g., an API
with rate limits). Use `max_concurrent_tasks` as a safety ceiling on total asyncio tasks.

## Connection Pooling

For high-throughput deployments, use a connection pool driver instead of a single connection:

```python
import asyncpg
from pgqueuer.db import AsyncpgPoolDriver

pool = await asyncpg.create_pool(min_size=5, max_size=20)
driver = AsyncpgPoolDriver(pool)
pgq = PgQueuer(driver)
```

**Pool sizing guidance:**
- A single `QueueManager` uses one connection for the LISTEN channel and borrows connections
  briefly for each dequeue/status-update round-trip.
- Start with `max_size = 2 × expected_concurrent_jobs` and adjust based on `pg_stat_activity`.
- Avoid pools larger than your PostgreSQL `max_connections` allows.

## Durability vs. Throughput

Choose the durability level that matches your risk tolerance:

| Level | Tables | Crash behaviour | Throughput |
|-------|--------|-----------------|------------|
| `durable` (default) | Logged (WAL) | No data loss | Baseline |
| `balanced` | Queue: unlogged; Log: logged | Queue data lost on crash | ~2× |
| `volatile` | All unlogged | All data lost on crash | Highest |

Change durability after installation without data loss:

```bash
pgq durability balanced
```

!!! warning
    `volatile` and `balanced` modes lose in-flight jobs on a PostgreSQL crash or restart.
    Use only for jobs that are safe to drop and re-enqueue (e.g., cache-warming, fire-and-forget
    analytics).

## Autovacuum Tuning

The `pgqueuer` table is high-churn: rows are inserted and then deleted rapidly. Without tuned
autovacuum, dead tuple bloat accumulates and slows down index scans.

Apply PgQueuer's recommended autovacuum settings:

```bash
pgq autovac
```

What this sets on `pgqueuer` and `pgqueuer_schedules` (high-churn tables):

| Setting | Value | Reason |
|---------|-------|--------|
| `autovacuum_vacuum_scale_factor` | `0.01` | Vacuum at 1% dead-tuple ratio |
| `autovacuum_vacuum_cost_limit` | `10000` | Aggressive vacuum speed |
| `autovacuum_vacuum_cost_delay` | `0` | No throttling |
| `fillfactor` | `70` | Leave 30% free for HOT updates |

`pgqueuer_log` uses conservative settings (vacuum at 95% dead-tuple ratio) since it is
append-only.

Revert to system defaults:

```bash
pgq autovac --rollback
```

## NOTIFY Channel and Polling Fallback

PgQueuer uses `LISTEN/NOTIFY` for near-instant job pickup. A trigger fires on every insert
into `pgqueuer` and sends a notification on the `ch_pgqueuer` channel.

**What can go wrong:** pgBouncer in transaction-pooling mode drops `LISTEN` subscriptions
between transactions. PgQueuer handles this in two ways:

1. **Polling fallback** — `QueueManager` re-polls after `dequeue_timeout` even without a
   NOTIFY, so jobs are never permanently stuck.
2. **Listener health check** — start with `--shutdown-on-listener-failure` so a supervisor
   can restart a manager whose LISTEN channel has become unhealthy:

```bash
pgq run myapp:main --shutdown-on-listener-failure
```

!!! tip "Use a dedicated connection for LISTEN"
    Always use a direct `asyncpg` connection (not a pool) for the `QueueManager`. Reserve
    the pool for producers. This avoids the LISTEN-loss problem entirely.

## Indexes

PgQueuer installs all required indexes automatically. The most important for throughput:

```sql
-- Used for every dequeue: priority-ordered job selection
CREATE INDEX ON pgqueuer (priority DESC, id ASC)
WHERE status = 'queued';

-- Used for heartbeat-based retry detection
CREATE INDEX ON pgqueuer (updated, id)
WHERE status = 'picked';
```

These partial indexes are maintained by `pgq install` and `pgq upgrade`. Do not drop them.

## Quick-Reference Checklist

- [ ] Run `pgq autovac` after installation
- [ ] Choose a `durability` level appropriate for your crash recovery requirements
- [ ] Use `AsyncpgPoolDriver` for producers; a single connection for `QueueManager`
- [ ] Set `retry_timer` to recover from worker crashes automatically
- [ ] Add `--shutdown-on-listener-failure` when running behind a PgBouncer pool
- [ ] Monitor `pgqueuer_log` table size and prune if needed
