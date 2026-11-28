WITH transitions AS (
    SELECT
        entrypoint,
        status,
        created,
        LAG(status) OVER w AS prev_status,
        LAG(created) OVER w AS prev_created
    FROM pgqueuer_log
    WHERE created > NOW() - $1::interval
    WINDOW w AS (PARTITION BY job_id ORDER BY created, id)
), durations AS (
    SELECT
        entrypoint,
        EXTRACT(EPOCH FROM created - prev_created) AS duration_seconds
    FROM transitions
    WHERE prev_status = 'picked'
      AND status IN ('successful', 'exception', 'canceled', 'failed')
)
SELECT
    entrypoint,
    COUNT(*) AS completed,
    percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_seconds) AS p50_seconds,
    percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_seconds) AS p95_seconds,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY duration_seconds) AS p99_seconds,
    MAX(duration_seconds) AS max_seconds
FROM durations
GROUP BY entrypoint
ORDER BY p95_seconds DESC
