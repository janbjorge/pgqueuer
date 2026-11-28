SELECT
    queue_manager_id,
    COUNT(*) AS active_jobs,
    MIN(heartbeat) AS oldest_heartbeat,
    MAX(heartbeat) AS newest_heartbeat,
    array_agg(DISTINCT entrypoint) AS entrypoints
FROM pgqueuer
WHERE status = 'picked'::pgqueuer_status
  AND queue_manager_id IS NOT NULL
GROUP BY queue_manager_id
ORDER BY queue_manager_id
