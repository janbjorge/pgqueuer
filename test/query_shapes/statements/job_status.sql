SELECT DISTINCT ON (job_id) job_id, status
FROM   pgqueuer_log
WHERE  job_id = ANY($1)
ORDER  BY job_id, created DESC, id DESC;
