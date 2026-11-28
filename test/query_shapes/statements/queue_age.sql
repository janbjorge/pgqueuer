SELECT
    entrypoint,
    COUNT(*) AS queued_count,
    MIN(created) AS oldest_created,
    EXTRACT(EPOCH FROM NOW() - MIN(created)) AS oldest_age_seconds,
    AVG(EXTRACT(EPOCH FROM NOW() - created)) AS avg_age_seconds
FROM pgqueuer
WHERE status = 'queued'::pgqueuer_status
GROUP BY entrypoint
ORDER BY entrypoint
