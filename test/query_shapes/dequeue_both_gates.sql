WITH
-- Unpack per-entrypoint parameters; concurrency_limit 0 means unlimited.
params AS (
    SELECT
        UNNEST($2::text[]) AS entrypoint,
        UNNEST($5::bigint[]) AS concurrency_limit
),

-- Per-entrypoint count of picked jobs (global, all workers).
picked AS (
    SELECT entrypoint, COUNT(*) AS total
    FROM pgqueuer
    WHERE queue_manager_id IS NOT NULL
      AND entrypoint = ANY($2)
    GROUP BY entrypoint
),

-- Entrypoints with free capacity; remaining caps a single dequeue.
available AS (
    SELECT
        params.entrypoint,
        CASE
            WHEN params.concurrency_limit <= 0 THEN NULL
            ELSE params.concurrency_limit - COALESCE(picked.total, 0)
        END AS remaining
    FROM params
    LEFT JOIN picked ON picked.entrypoint = params.entrypoint
    WHERE params.concurrency_limit <= 0
       OR COALESCE(picked.total, 0) < params.concurrency_limit
),

-- This worker's total picked jobs (scalar, for max_concurrent_tasks).
worker_load AS (
    SELECT COUNT(*) AS total
    FROM pgqueuer
    WHERE queue_manager_id = $3
      AND entrypoint = ANY($2)
),

-- Claims for entrypoints without a limit; lock straight down the backlog.
unlimited_claims AS (
    SELECT job.id, job.priority
    FROM available
    CROSS JOIN LATERAL (
        SELECT candidate.id, candidate.priority
        FROM pgqueuer candidate
        WHERE candidate.entrypoint = available.entrypoint
          AND candidate.status = 'queued'
          AND candidate.execute_after < NOW()
        ORDER BY candidate.priority DESC, candidate.id ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
    ) job
    WHERE available.remaining IS NULL
),

-- Claims for limited entrypoints; the window is fixed before the lock.
limited_claims AS (
    SELECT job.id, job.priority
    FROM available
    CROSS JOIN LATERAL (
        SELECT locked.id, locked.priority
        FROM (
            SELECT candidate.id
            FROM pgqueuer candidate
            WHERE candidate.entrypoint = available.entrypoint
              AND candidate.status = 'queued'
              AND candidate.execute_after < NOW()
            ORDER BY candidate.priority DESC, candidate.id ASC
            LIMIT LEAST($1, available.remaining)
        ) capped
        CROSS JOIN LATERAL (
            SELECT target.id, target.priority
            FROM pgqueuer target
            WHERE target.id = capped.id
              AND target.status = 'queued'
              AND target.execute_after < NOW()
            FOR UPDATE OF target SKIP LOCKED
        ) locked
        ORDER BY locked.priority DESC, locked.id ASC
        LIMIT LEAST($1, available.remaining)
    ) job
    WHERE available.remaining IS NOT NULL
),

-- Both claim sets merged into one priority order.
next_queued AS (
    SELECT job.id, job.priority
    FROM (
        SELECT id, priority FROM unlimited_claims
        UNION ALL
        SELECT id, priority FROM limited_claims
    ) job
    WHERE (SELECT total FROM worker_load) < $6
    ORDER BY job.priority DESC, job.id ASC
    LIMIT $1
),

-- Stale picked jobs whose heartbeat timed out.
next_stale AS (
    SELECT stale.id, stale.priority
    FROM pgqueuer stale
    WHERE stale.status = 'picked'
      AND stale.entrypoint = ANY($2)
      AND stale.heartbeat < NOW() - $4::interval
      AND stale.execute_after < NOW()
      AND (SELECT total FROM worker_load) < $6
    ORDER BY stale.priority DESC, stale.id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT $1
),

-- Merge both sets into one priority order so stale competes with fresh.
eligible AS (
    SELECT id FROM (
        SELECT id, priority FROM next_queued
        UNION ALL
        SELECT id, priority FROM next_stale
    ) combined
    ORDER BY priority DESC, id ASC
    LIMIT GREATEST(LEAST($1, $6 - (SELECT total FROM worker_load)), 0)
),

-- Atomically claim the jobs and log the pick event.
claimed AS (
    UPDATE pgqueuer
    SET status = 'picked',
        updated   = NOW(),
        heartbeat = NOW(),
        queue_manager_id = $3
    WHERE id IN (SELECT id FROM eligible)
    RETURNING *
),

log_pick AS (
    INSERT INTO pgqueuer_log (job_id, status, entrypoint, priority)
    SELECT id, status, entrypoint, priority FROM claimed
)
SELECT * FROM claimed ORDER BY priority DESC, id ASC;
