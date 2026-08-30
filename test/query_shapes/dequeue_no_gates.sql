WITH
-- No entrypoint carries a concurrency limit; all are available.
available AS (
    SELECT UNNEST($2::text[]) AS entrypoint
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
          AND candidate.execute_after < NOW()
        ORDER BY candidate.priority DESC, candidate.id ASC
        LIMIT $1
        FOR UPDATE SKIP LOCKED
    ) job
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
    LIMIT $1
),

-- Claim every eligible job in one atomic UPDATE.
claimed AS (
    UPDATE pgqueuer
    SET status = 'picked',
        updated   = NOW(),
        heartbeat = NOW(),
        queue_manager_id = $3
    WHERE id IN (SELECT id FROM eligible)
    RETURNING *
),

-- Record the pick in the log table.
log_pick AS (
    INSERT INTO pgqueuer_log (job_id, status, entrypoint, priority)
    SELECT id, status, entrypoint, priority FROM claimed
)
SELECT * FROM claimed ORDER BY priority DESC, id ASC;
