SELECT id, priority, queue_manager_id, created, updated, heartbeat,
    execute_after, status, entrypoint,
    EXTRACT(EPOCH FROM NOW() - heartbeat) AS seconds_since_heartbeat
FROM pgqueuer
WHERE status = 'picked'::pgqueuer_status
  AND heartbeat < NOW() - $1::interval
ORDER BY heartbeat ASC
LIMIT $2
