SELECT * FROM pgqueuer
WHERE status = 'failed'
ORDER BY created DESC
LIMIT $1
