WITH
-- No entrypoint carries a concurrency limit; all are available.
available AS (
    SELECT UNNEST($1::text[]) AS entrypoint
),

-- This worker's picked jobs, and how many more this batch may claim.
worker_load AS (
    SELECT
        COUNT(*) AS total,
        GREATEST(LEAST(10, $4 - COUNT(*)), 0) AS headroom
    FROM pgqueuer
    WHERE queue_manager_id = $2
      AND entrypoint = ANY($1)
),

-- New queued jobs; LATERAL hits the (entrypoint, priority, id) index.
next_queued AS (
    SELECT job.id, job.priority
    FROM available
    CROSS JOIN LATERAL (
        SELECT candidate.id, candidate.priority
        FROM pgqueuer candidate
        WHERE candidate.entrypoint = available.entrypoint
          AND candidate.status = 'queued'
          AND candidate.execute_after <= NOW()
        ORDER BY candidate.priority DESC, candidate.id ASC
        LIMIT 10
        FOR UPDATE SKIP LOCKED
    ) job
    WHERE (SELECT total FROM worker_load) < $4
    ORDER BY job.priority DESC, job.id ASC
    LIMIT 10
),

-- Stale picked jobs whose heartbeat timed out.
next_stale AS (
    SELECT stale.id, stale.priority, stale.entrypoint
    FROM pgqueuer stale
    WHERE stale.status = 'picked'
      AND stale.entrypoint = ANY($1)
      AND stale.heartbeat < NOW() - $3::interval
      AND stale.execute_after <= NOW()
      AND (SELECT total FROM worker_load) < $4
    ORDER BY stale.priority DESC, stale.id ASC
    FOR UPDATE SKIP LOCKED
    LIMIT 10
),

-- Merge both sets into one priority order so stale competes with fresh.
eligible AS (
    SELECT id FROM (
        SELECT id, priority FROM next_queued
        UNION ALL
        SELECT id, priority FROM next_stale
    ) combined
    ORDER BY priority DESC, id ASC
    LIMIT (SELECT headroom FROM worker_load)
),

-- Claim every eligible job in one atomic UPDATE.
claimed AS (
    UPDATE pgqueuer
    SET status = 'picked',
        updated   = NOW(),
        heartbeat = NOW(),
        -- Taking over another manager's stale row counts as a failed attempt.
        attempts  = attempts + CASE WHEN status = 'picked'
            AND queue_manager_id IS DISTINCT FROM $2
            THEN 1 ELSE 0 END,
        queue_manager_id = $2
    WHERE id IN (SELECT id FROM eligible)
    RETURNING *
),

-- Record the pick in the log table.
log_pick AS (
    INSERT INTO pgqueuer_log (job_id, status, entrypoint, priority)
    SELECT id, status, entrypoint, priority FROM claimed
)
SELECT * FROM claimed ORDER BY priority DESC, id ASC;
