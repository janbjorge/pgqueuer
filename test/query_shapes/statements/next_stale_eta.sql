SELECT GREATEST(heartbeat + $3::interval - NOW(), INTERVAL '0') AS eta
FROM pgqueuer
WHERE status = 'picked'
  AND entrypoint = ANY($1)
  AND queue_manager_id IS DISTINCT FROM $2
ORDER BY heartbeat
LIMIT 1
