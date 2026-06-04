# Row Locking & `FOR UPDATE SKIP LOCKED`

PgQueuer turns a regular PostgreSQL table into a job queue. The single mechanism
that makes that safe under many concurrent workers is the locking clause
`FOR UPDATE SKIP LOCKED`. This page is a deep dive into *why* that clause exists,
*what* PostgreSQL actually does when it runs, and *how* PgQueuer's dequeue query
uses it. If you only remember one sentence, make it this one, straight from the
PostgreSQL manual:

> With `SKIP LOCKED`, any selected rows that cannot be immediately locked are
> skipped. [...] this is not suitable for general purpose work, but can be used
> to avoid lock contention with multiple consumers accessing a queue-like table.
>
> — [PostgreSQL: SELECT — The Locking Clause](https://www.postgresql.org/docs/current/sql-select.html)

A queue-like table is exactly our use case.

---

## The problem: many workers, one table

Picture `N` worker processes, each polling the same `pgqueuer` table for the next
`queued` job. They must agree on a rule that never lets two workers run the *same*
job, while still letting all `N` of them make progress in parallel. Without help
from the database, every naive approach fails:

```
                    pgqueuer table
                  ┌──────────────────┐
   worker A ─────▶│ id=1  queued     │◀───── worker B
   worker B ─────▶│ id=2  queued     │◀───── worker C
   worker C ─────▶│ id=3  queued     │◀───── worker A
                  │ ...              │
                  └──────────────────┘
        Who gets id=1? How do the others NOT also get id=1?
```

`SELECT ... LIMIT 1` on its own is useless here: every worker reads the same
top row because plain reads do not coordinate. We need the workers to *claim*
rows, and we need a claim to be visible to the other workers immediately.

---

## Background: MVCC, and why plain reads don't help

PostgreSQL uses **Multi-Version Concurrency Control (MVCC)**. Each statement sees
a snapshot of the database as of a point in time. The headline property is:

> reading never blocks writing and writing never blocks reading.
>
> — [PostgreSQL: Introduction to MVCC](https://www.postgresql.org/docs/current/mvcc-intro.html)

That property is wonderful for OLTP and terrible for queue coordination. Because
a plain `SELECT` never blocks and never takes a row lock, two workers running
`SELECT id FROM pgqueuer WHERE status='queued' ORDER BY id LIMIT 1` at the same
time both read `id=1`. MVCC isolation guarantees they each see a consistent
snapshot — it does **not** guarantee they see *different* rows. To make workers
coordinate we have to step outside pure MVCC and ask for **explicit row-level
locks**.

---

## Row-level lock modes

The locking clause is appended to a `SELECT` and has the form:

```
FOR { UPDATE | NO KEY UPDATE | SHARE | KEY SHARE } [ OF table ... ] [ NOWAIT | SKIP LOCKED ]
```

PostgreSQL defines four lock *strengths*. From strongest to weakest:

| Mode | Acquired by | Blocks |
|------|-------------|--------|
| `FOR UPDATE` | `SELECT FOR UPDATE`, `DELETE`, key-changing `UPDATE` | every other row lock, incl. `FOR KEY SHARE` |
| `FOR NO KEY UPDATE` | ordinary `UPDATE` | everything except `FOR KEY SHARE` |
| `FOR SHARE` | `SELECT FOR SHARE` | `UPDATE`/`DELETE`/`FOR UPDATE`/`FOR NO KEY UPDATE` |
| `FOR KEY SHARE` | foreign-key checks | only `FOR UPDATE` and key changes |

The exact conflict matrix from the manual (`X` = the two conflict and cannot be
held simultaneously by different transactions):

```
                     held lock
 requested        KEY SHARE  SHARE  NO KEY UPDATE  UPDATE
 ───────────────────────────────────────────────────────
 FOR KEY SHARE        ·        ·          ·          X
 FOR SHARE            ·        ·          X          X
 FOR NO KEY UPDATE    ·        X          X          X
 FOR UPDATE           X        X          X          X
```

PgQueuer always uses **`FOR UPDATE`** — the strongest mode — because claiming a
job is a write intent: the very next thing the query does is `UPDATE` the row to
`status='picked'`. Two important facts about these locks:

- **They are held until the end of the transaction.** "Row-level locks are
  released at transaction end or during savepoint rollback." A claim is only
  durable once committed.
- **They do not block plain readers.** "Row-level locks do not affect data
  querying; they block only *writers and lockers* to the same row." A dashboard
  running `SELECT count(*)` is never blocked by an in-flight dequeue.

---

## Three ways to react to a locked row

When your `SELECT ... FOR UPDATE` reaches a row another transaction has already
locked, you choose the behavior with the trailing option:

```
   ┌─────────────────────────── row is already locked ──────────────────────────┐
   │                                                                             │
   ▼                              ▼                                ▼
(default)                      NOWAIT                         SKIP LOCKED
   │                              │                                │
   ▼                              ▼                                ▼
 WAIT for the lock           ERROR immediately               IGNORE the row,
 to be released              (SQLSTATE 55P03)                 move to the next
 (block indefinitely)        "could not obtain lock"         one as if invisible
```

- **Default — wait.** "A transaction seeking [...] a row-level lock will wait
  indefinitely for conflicting locks to be released." Good for correctness in
  general OLTP, fatal for a queue: workers would line up behind each other.
- **`NOWAIT` — fail fast.** Raises an error rather than waiting. Useful when you
  want to detect contention, but it forces the application to catch and retry.
- **`SKIP LOCKED` — step over.** Locked rows are treated *as if they did not
  exist* for this query. No waiting, no error, no retry loop. This is what a
  queue wants.

---

## How `SKIP LOCKED` distributes work

`SKIP LOCKED` tries to lock each candidate row; if the lock can't be taken
*immediately*, it drops that row from the result and keeps scanning. Two workers
issuing the same query at the same instant therefore walk past each other's
in-flight rows and each come away with a different set.

Adapting Michael Paquier's two-session demonstration to a queue table:

```
 time   worker A                              worker B
 ────────────────────────────────────────────────────────────────────────────
  t0     BEGIN                                BEGIN
  t1     SELECT id FROM q                     SELECT id FROM q
         WHERE status='queued'               WHERE status='queued'
         ORDER BY id                         ORDER BY id
         FOR UPDATE SKIP LOCKED LIMIT 2      FOR UPDATE SKIP LOCKED LIMIT 2
  t2     locks id=1  ✓                        id=1 locked by A → skip
         locks id=2  ✓                        id=2 locked by A → skip
         ── returns {1,2} ──                  locks id=3  ✓
                                              locks id=4  ✓
                                              ── returns {3,4} ──
  t3     UPDATE ... SET status='picked'       UPDATE ... SET status='picked'
         WHERE id IN (1,2)                    WHERE id IN (3,4)
  t4     COMMIT  (locks on 1,2 released)      COMMIT  (locks on 3,4 released)
```

No worker ever blocks on another, and no job is handed out twice. Throughput
scales with the number of workers instead of collapsing to one-at-a-time.

!!! note "`SKIP LOCKED` returns an *inconsistent* view on purpose"
    The manual is explicit: "Skipping locked rows provides an inconsistent view
    of the data." Worker B above genuinely cannot see jobs 1 and 2, even though
    they exist and are `queued`. For a queue that is precisely the desired
    behavior — those jobs belong to someone else right now — but it is the reason
    the manual warns against using `SKIP LOCKED` for general-purpose queries.

---

## Why the *naive* queue pattern is wrong

A very common first attempt looks like this:

```sql
-- DON'T: plain FOR UPDATE, no SKIP LOCKED
SELECT id FROM pgqueuer
WHERE status = 'queued'
ORDER BY id
LIMIT 1
FOR UPDATE;
```

With plain `FOR UPDATE` (the default *wait* behavior), all `N` workers sort the
same way, all target the same top row, and `N-1` of them **block** on the one
that won the row. They form a convoy:

```
 worker A ─lock id=1─▶ [runs job]
 worker B ──────────── waits on id=1 ──────────┐
 worker C ──────────── waits on id=1 ──────────┤  all serialized
 worker D ──────────── waits on id=1 ──────────┘  behind worker A
                         │
            A commits ───┘  now ONE of B/C/D wakes, re-evaluates,
                            and the convoy reforms on the next row
```

The queue degrades to single-file processing no matter how many workers you add,
and latency spikes under load. This is the failure mode Craig Ringer summarized
in the title of his canonical write-up: *"What is `SKIP LOCKED` for in
PostgreSQL? Most work-queue implementations are wrong."* Swapping in
`SKIP LOCKED` is what makes the workers fan out instead of queue up.

---

## The correct pattern: lock in a subquery, then `UPDATE`

The robust dequeue idiom claims rows inside a subquery and updates them in the
same statement:

```sql
UPDATE pgqueuer
SET status = 'picked'
WHERE id IN (
    SELECT id FROM pgqueuer
    WHERE status = 'queued'
    ORDER BY priority DESC, id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $batch_size
)
RETURNING *;
```

Doing it in one statement means the claim (`status='picked'`) and the lock live
in the same transaction, so the window where a row is "locked but not yet
claimed" is as small as possible and disappears entirely on commit.

### Mind the `ORDER BY` + `LIMIT` + locking caution

There is one sharp edge the manual calls out. Under `READ COMMITTED`:

> It is possible for a `SELECT` command running at the `READ COMMITTED`
> transaction isolation level and using `ORDER BY` and a locking clause to return
> rows out of order. This is because `ORDER BY` is applied first. The command
> sorts the result, but might then block trying to obtain a lock [...]

`SKIP LOCKED` sidesteps the *blocking* half of that hazard (it never waits), but
the ordering-then-locking sequence is still why the subquery form is preferred:
the inner `SELECT` picks and locks the candidate ids, and the outer `UPDATE`
operates on exactly those ids. Two more documented details worth knowing:

- **`LIMIT` stops locking early.** "If a `LIMIT` is used, locking stops once
  enough rows have been returned to satisfy the limit" — so `LIMIT $batch_size`
  bounds how many rows you lock, not just how many you return.
- **`OFFSET` still locks skipped rows.** "Rows skipped over by `OFFSET` will get
  locked." Avoid `OFFSET` in a dequeue path; it locks rows you never process.

---

## How PgQueuer applies all of this

PgQueuer's real dequeue query (built in
[`build_dequeue_query`](https://github.com/janbjorge/pgqueuer/blob/main/pgqueuer/adapters/persistence/qb.py))
is a single statement assembled from several CTEs. Stripped to its locking
skeleton it is exactly the correct pattern above:

```sql
WITH
next_queued AS (
    SELECT q.id
    FROM pgqueuer q
    WHERE q.status = 'queued'
      AND q.execute_after < NOW()
      -- ... concurrency-limit gates ...
    ORDER BY q.priority DESC, q.id ASC
    FOR UPDATE SKIP LOCKED          -- ← fan-out happens here
    LIMIT $1
),
next_stale AS (
    SELECT q.id
    FROM pgqueuer q
    WHERE q.status = 'picked'
      AND q.heartbeat < NOW() - $6::interval   -- crashed worker's jobs
    ORDER BY q.priority DESC, q.id ASC
    FOR UPDATE SKIP LOCKED          -- ← also skip-locked
    LIMIT $1
),
eligible AS (        -- merge fresh + recovered, capped to batch size
    SELECT id FROM (
        SELECT id, 0 AS src FROM next_queued
        UNION ALL
        SELECT id, 1 AS src FROM next_stale
    ) combined
    ORDER BY src, id
    LIMIT $1
),
claimed AS (         -- atomic claim, same transaction as the lock
    UPDATE pgqueuer
    SET status = 'picked', updated = NOW(), heartbeat = NOW(),
        queue_manager_id = $4
    WHERE id IN (SELECT id FROM eligible)
    RETURNING *
)
SELECT * FROM claimed ORDER BY priority DESC, id ASC;
```

Things to notice, tying back to the concepts above:

1. **Two `FOR UPDATE SKIP LOCKED` scans.** One for fresh `queued` work, one for
   `picked` jobs whose `heartbeat` has gone stale (a worker that crashed
   mid-job). Both use `SKIP LOCKED` so a recovery scan never blocks on jobs that
   are still being actively processed by a live worker.
2. **`ORDER BY priority DESC, id ASC` inside the locked subqueries.** Higher
   priority first, FIFO within a priority. Because the lock and claim happen in
   one statement, the `ORDER BY`-then-lock caution is contained.
3. **`LIMIT $1` is `batch_size`.** Per the manual, this also bounds how many
   rows the statement locks — see
   [Performance Tuning](../guides/performance-tuning.md) for choosing it.
4. **The `UPDATE ... RETURNING` is the claim.** Locking and flipping
   `status='picked'` in the same statement means that once the transaction
   commits, the rows are unambiguously owned; the row locks are released at that
   commit, and the `status='picked'` (plus `queue_manager_id` and `heartbeat`)
   is what keeps any *other* worker from re-selecting them.
5. **Stale recovery instead of stuck jobs.** Because a row lock is dropped if the
   owning transaction/connection dies, and the claim is recorded as durable
   `status='picked'` state, a crashed worker leaves a `picked` row behind. The
   `heartbeat` timeout — not the lock — is what lets `next_stale` reclaim it.
   See [Heartbeat Monitoring](../guides/heartbeat.md).

---

## Summary

| Concern | Mechanism |
|---------|-----------|
| Two workers must not run the same job | row-level `FOR UPDATE` lock = exclusive claim intent |
| Workers must not block each other | `SKIP LOCKED` steps over in-flight rows |
| Don't wait, don't error on contention | `SKIP LOCKED` (vs. default wait / `NOWAIT` error) |
| Claim must survive other workers | `UPDATE ... status='picked'` in the same transaction |
| Bounded work per round trip | `LIMIT $batch_size` also bounds rows locked |
| Recover a crashed worker's jobs | `heartbeat` timeout + second `SKIP LOCKED` scan |
| Plain `SELECT`/dashboards unaffected | row locks block writers/lockers only, never readers |

`FOR UPDATE SKIP LOCKED` is the small clause that lets PgQueuer treat an ordinary
table as a high-throughput, multi-consumer queue without a single line of
application-side locking code.

---

## Sources

- [PostgreSQL Manual — SELECT, The Locking Clause](https://www.postgresql.org/docs/current/sql-select.html) (`SKIP LOCKED`, `NOWAIT`, `LIMIT`/`OFFSET` and `ORDER BY` cautions)
- [PostgreSQL Manual — 13.3. Explicit Locking](https://www.postgresql.org/docs/current/explicit-locking.html) (row-lock modes, conflict matrix, lock duration, readers vs. writers)
- [PostgreSQL Manual — 13.1. Introduction to MVCC](https://www.postgresql.org/docs/current/mvcc-intro.html)
- Craig Ringer, *What is SKIP LOCKED for in PostgreSQL? Most work-queue implementations are wrong* (2ndQuadrant / EnterpriseDB) — [discussion on Lobsters](https://lobste.rs/s/fyakws/what_is_skip_locked_for_postgresql_most)
- Michael Paquier, [*Postgres 9.5 feature highlight — SKIP LOCKED for row-level locking*](https://paquier.xyz/postgresql-2/postgres-9-5-feature-highlight-skip-locked-row-level/)
