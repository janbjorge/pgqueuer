SELECT * FROM pgqueuer_log
WHERE job_id = $1::bigint
ORDER BY created ASC, id ASC
LIMIT $2
