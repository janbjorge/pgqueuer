WITH params AS (
    SELECT UNNEST($1::text[])       AS expression,
           UNNEST($2::text[])       AS entrypoint,
           UNNEST($3::interval[])   AS delay
), picked_jobs AS (
    SELECT id, expression, entrypoint
    FROM pgqueuer_schedules
    WHERE
            ((entrypoint, expression) IN (SELECT entrypoint, expression FROM params)
        AND next_run <= NOW()
        AND status = 'queued')
        OR
            ((entrypoint, expression) IN (SELECT entrypoint, expression FROM params)
        AND NOW() - heartbeat > interval '30 seconds'
        AND status = 'picked')
    FOR UPDATE SKIP LOCKED
)
UPDATE pgqueuer_schedules
SET
    status = 'picked',
    updated = NOW(),
    heartbeat = NOW(),
    next_run = NOW() + (
            SELECT delay FROM params
            WHERE
                params.entrypoint = pgqueuer_schedules.entrypoint
            AND params.expression = pgqueuer_schedules.expression
        )
WHERE (entrypoint, expression) IN (SELECT entrypoint, expression FROM picked_jobs)
RETURNING *;
