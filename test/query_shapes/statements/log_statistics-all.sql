-- statement 1
WITH guard AS (
    SELECT TRUE AS proceed
),
updated_logs AS (
    UPDATE pgqueuer_log
    SET aggregated = TRUE
    FROM guard
    WHERE NOT aggregated AND guard.proceed
    RETURNING entrypoint, priority, status, created
),
log_aggregation AS (
    SELECT
        entrypoint,
        priority,
        status,
        COUNT(*) AS count,
        date_trunc('sec', created) AS created
    FROM updated_logs
    GROUP BY entrypoint, priority, status, date_trunc('sec', created)
)
INSERT INTO pgqueuer_statistics (count, created, entrypoint, priority, status)
SELECT count, created, entrypoint, priority, status
FROM log_aggregation
ON CONFLICT (priority, date_trunc('sec'::text, timezone('UTC'::text, created)), status, entrypoint) DO UPDATE SET
    count = pgqueuer_statistics.count + EXCLUDED.count

-- statement 2
SELECT count, created, entrypoint, priority, status
FROM pgqueuer_statistics
ORDER BY id DESC
