SELECT * FROM pgqueuer
WHERE status = 'failed'
ORDER BY created ASC
LIMIT $1
