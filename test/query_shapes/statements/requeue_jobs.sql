WITH requeued AS (
    UPDATE pgqueuer
    SET status = 'queued', execute_after = NOW(), updated = NOW(),
        attempts = 0, queue_manager_id = NULL
    WHERE id = ANY($1::bigint[]) AND status = 'failed'
    RETURNING id, entrypoint, priority
)
INSERT INTO pgqueuer_log (job_id, status, entrypoint, priority)
SELECT id, 'queued', entrypoint, priority FROM requeued
RETURNING job_id
