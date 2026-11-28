WITH job_status AS (
    SELECT
        UNNEST($1::bigint[])                            AS id,
        UNNEST($2::pgqueuer_status[]) AS status,
        UNNEST($3::JSONB[])                             AS traceback,
        UNNEST($4::uuid[])                              AS queue_manager_id
), deleted AS (
    DELETE FROM pgqueuer q
    USING job_status js
    WHERE q.id = js.id
        AND js.status != 'failed'
        AND (
            q.queue_manager_id = js.queue_manager_id
            OR (js.queue_manager_id IS NULL AND q.status <> 'picked')
        )
    RETURNING q.id, q.entrypoint, q.priority
), held AS (
    UPDATE pgqueuer q
    SET status = 'failed', updated = NOW(), queue_manager_id = NULL
    FROM job_status js
    WHERE q.id = js.id
        AND js.status = 'failed'
        AND (js.queue_manager_id IS NULL OR q.queue_manager_id = js.queue_manager_id)
    RETURNING q.id, q.entrypoint, q.priority
), all_resolved AS (
    SELECT id, entrypoint, priority FROM deleted
    UNION ALL
    SELECT id, entrypoint, priority FROM held
), merged AS (
    SELECT
        job_status.id           AS id,
        job_status.status       AS status,
        job_status.traceback    AS traceback,
        all_resolved.entrypoint AS entrypoint,
        all_resolved.priority   AS priority
    FROM job_status
    INNER JOIN all_resolved
        ON all_resolved.id = job_status.id
)
INSERT INTO pgqueuer_log (
    job_id,
    status,
    entrypoint,
    priority,
    traceback
)
SELECT id, status, entrypoint, priority, traceback FROM merged
