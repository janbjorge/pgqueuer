# Row Locking & `FOR UPDATE SKIP LOCKED`

PgQueuer treats an ordinary PostgreSQL table as a job queue. `FOR UPDATE SKIP
LOCKED` is the clause that lets many workers share that table without running the
same job twice and without blocking each other.

> With `SKIP LOCKED`, any selected rows that cannot be immediately locked are
> skipped. [...] this is not suitable for general purpose work, but can be used
> to avoid lock contention with multiple consumers accessing a queue-like
> table.[^locking]

---

## Why plain reads don't work

PostgreSQL's MVCC means a plain `SELECT` never blocks and never locks a row:
"reading never blocks writing and writing never blocks reading."[^mvcc] So two
workers running the same `SELECT ... LIMIT 1` both read `id=1`. MVCC gives each a
consistent snapshot; it does **not** hand them *different* rows. Coordinating
workers requires an explicit **row-level lock**.

---

## Lock modes

```
FOR { UPDATE | NO KEY UPDATE | SHARE | KEY SHARE } [ NOWAIT | SKIP LOCKED ]
```

Four strengths, with this conflict matrix (`X` = cannot be held together by
different transactions):[^explicit]

```
                     held lock
 requested        KEY SHARE  SHARE  NO KEY UPDATE  UPDATE
 ───────────────────────────────────────────────────────
 FOR KEY SHARE        ·        ·          ·          X
 FOR SHARE            ·        ·          X          X
 FOR NO KEY UPDATE    ·        X          X          X
 FOR UPDATE           X        X          X          X
```

PgQueuer uses `FOR UPDATE` (strongest) because a claim is a write intent: the
next step is `UPDATE ... status='picked'`. Two facts about these locks:[^explicit]

- Held until the transaction ends; a claim is durable only after commit.
- They block writers/lockers, never plain readers: a dashboard `SELECT count(*)`
  is never blocked by a dequeue.

---

## Reacting to a locked row

```
              row already locked
        ┌────────────┼────────────┐
        ▼            ▼             ▼
    (default)      NOWAIT      SKIP LOCKED
     WAIT          ERROR        skip it,
   forever       immediately   keep scanning
```

- **default**: waits indefinitely.[^explicit] Fatal for a queue: workers convoy.
- **`NOWAIT`**: errors instead of waiting; forces app-side retry.
- **`SKIP LOCKED`**: locked rows are treated as nonexistent. No wait, no error.

---

## How `SKIP LOCKED` distributes work

Each candidate row is locked if possible; rows it can't lock immediately are
dropped. Two workers issuing the same query fan out onto different rows:[^paquier]

```
 worker A                          worker B
 ─────────────────────────────────────────────────────────
 FOR UPDATE SKIP LOCKED LIMIT 2    FOR UPDATE SKIP LOCKED LIMIT 2
 locks 1,2  → returns {1,2}        1,2 locked by A → skip
                                   locks 3,4  → returns {3,4}
 UPDATE ... WHERE id IN (1,2)      UPDATE ... WHERE id IN (3,4)
 COMMIT (release 1,2)              COMMIT (release 3,4)
```

No worker blocks another; no job is handed out twice.

!!! note "Inconsistent by design"
    `SKIP LOCKED` "provides an inconsistent view of the data."[^locking] Worker B
    genuinely can't see jobs 1 and 2: exactly what a queue wants, and why the
    manual warns against it for general-purpose queries.

---

## The naive pattern is wrong

```sql
-- DON'T: plain FOR UPDATE
SELECT id FROM pgqueuer WHERE status='queued' ORDER BY id LIMIT 1 FOR UPDATE;
```

All workers target the same top row; `N-1` block on the winner and process
single-file no matter how many you add: the failure mode behind Craig Ringer's
"most work-queue implementations are wrong."[^ringer] `SKIP LOCKED` fixes it.

---

## The correct pattern

Lock candidates in a subquery, claim them in the same statement:

```sql
UPDATE pgqueuer SET status='picked'
WHERE id IN (
    SELECT id FROM pgqueuer
    WHERE status='queued'
    ORDER BY priority DESC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $batch_size
)
RETURNING *;
```

Lock and claim share one transaction, so the "locked but unclaimed" window closes
on commit. Three documented details:[^locking]

- `ORDER BY` is applied *before* locking, so `ORDER BY` + a locking clause can
  return rows out of order under `READ COMMITTED`. The subquery form contains it.
- `LIMIT` stops locking once satisfied, so `LIMIT $batch_size` also bounds rows
  locked.
- `OFFSET` still locks skipped rows; avoid it in a dequeue path.

---

## In PgQueuer

[`build_dequeue_query`](https://github.com/janbjorge/pgqueuer/blob/main/pgqueuer/adapters/persistence/qb.py)
is the correct pattern, twice (simplified below; the real query adds
concurrency-limit gating and a pick-logging CTE):

```sql
WITH
next_queued AS (        -- fresh work
    SELECT q.id FROM pgqueuer q
    WHERE q.status='queued' AND q.execute_after < NOW()
    ORDER BY q.priority DESC, q.id ASC
    FOR UPDATE SKIP LOCKED LIMIT $1
),
next_stale AS (         -- jobs from a crashed worker
    SELECT q.id FROM pgqueuer q
    WHERE q.status='picked' AND q.heartbeat < NOW() - $6::interval
    ORDER BY q.priority DESC, q.id ASC
    FOR UPDATE SKIP LOCKED LIMIT $1
),
eligible AS (           -- merge, cap to batch size
    SELECT id FROM (
        SELECT id, 0 AS src FROM next_queued
        UNION ALL SELECT id, 1 AS src FROM next_stale
    ) c ORDER BY src, id LIMIT $1
),
claimed AS (            -- atomic claim
    UPDATE pgqueuer SET status='picked', updated=NOW(), heartbeat=NOW(),
        queue_manager_id=$4
    WHERE id IN (SELECT id FROM eligible) RETURNING *
)
SELECT * FROM claimed ORDER BY priority DESC, id ASC;
```

- **Two scans, both `SKIP LOCKED`**: fresh `queued` work plus `picked` jobs with
  a stale `heartbeat`. Recovery never blocks on jobs a live worker still holds.
- **The `UPDATE ... RETURNING` is the claim.** After commit, `status='picked'` +
  `queue_manager_id` keep other workers off the rows; the locks themselves are
  already released.
- **Stale recovery, not stuck jobs.** A crashed worker drops its locks but leaves
  a `picked` row; the `heartbeat` timeout, not the lock, lets `next_stale`
  reclaim it. See [Heartbeat Monitoring](../guides/heartbeat.md).
- **`LIMIT $1` is `batch_size`**: also the bound on rows locked. See
  [Performance Tuning](../guides/performance-tuning.md).

---

## Sources

[^locking]: PostgreSQL, [SELECT — The Locking Clause](https://www.postgresql.org/docs/current/sql-select.html): `SKIP LOCKED`/`NOWAIT` semantics, the "inconsistent view" warning, the `ORDER BY` + locking caution under `READ COMMITTED`, and `LIMIT`/`OFFSET` locking behavior.
[^mvcc]: PostgreSQL, [13.1. Introduction (MVCC)](https://www.postgresql.org/docs/current/mvcc-intro.html): "reading never blocks writing and writing never blocks reading."
[^explicit]: PostgreSQL, [13.3. Explicit Locking](https://www.postgresql.org/docs/current/explicit-locking.html): lock modes, the conflict matrix (Table 13.3), lock duration, readers vs. writers, and default wait behavior.
[^ringer]: Craig Ringer, *What is SKIP LOCKED for in PostgreSQL? Most work-queue implementations are wrong* (2ndQuadrant / EnterpriseDB); see the [Lobsters thread](https://lobste.rs/s/fyakws/what_is_skip_locked_for_postgresql_most).
[^paquier]: Michael Paquier, [*Postgres 9.5 feature highlight — SKIP LOCKED*](https://paquier.xyz/postgresql-2/postgres-9-5-feature-highlight-skip-locked-row-level/).
