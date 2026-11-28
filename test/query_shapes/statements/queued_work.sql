SELECT COUNT(*) AS queued_work
FROM (
    SELECT 1
    FROM pgqueuer
    WHERE entrypoint = ANY($1)
      AND status = 'queued'
    LIMIT 1
) sub
