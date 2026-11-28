WITH retry AS (
    UPDATE pgqueuer
    SET
        status = 'queued',
        execute_after = NOW() + $2::interval,
        attempts = attempts + 1,
        updated = NOW(),
        queue_manager_id = NULL
    WHERE id = $1::bigint
        AND ($4::uuid IS NULL OR queue_manager_id = $4::uuid)
    RETURNING id, entrypoint, priority
)
INSERT INTO pgqueuer_log (job_id, status, entrypoint, priority, traceback)
SELECT id, 'queued', entrypoint, priority, $3::JSONB FROM retry
RETURNING job_id
