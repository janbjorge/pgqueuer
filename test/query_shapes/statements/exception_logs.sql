SELECT * FROM pgqueuer_log
WHERE status = 'exception'::pgqueuer_status
ORDER BY created DESC
LIMIT $1
