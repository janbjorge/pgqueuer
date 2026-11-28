SELECT MIN(soonest.eta) AS eta
FROM UNNEST($1::text[]) AS wanted(entrypoint)
CROSS JOIN LATERAL (
    SELECT job.execute_after - NOW() AS eta
    FROM pgqueuer job
    WHERE job.status = 'queued'
      AND job.entrypoint = wanted.entrypoint
      AND NOT (job.execute_after <= NOW())
    ORDER BY job.execute_after
    LIMIT 1
) soonest
