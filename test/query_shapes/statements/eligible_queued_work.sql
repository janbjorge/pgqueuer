SELECT COUNT(*) AS queued_work
FROM (
    SELECT 1
    FROM UNNEST($1::text[], $2::bigint[]) AS params(entrypoint, concurrency_limit)
    WHERE (
        params.concurrency_limit <= 0
        OR (
            SELECT COUNT(*)
            FROM pgqueuer p
            WHERE p.entrypoint = params.entrypoint
              AND p.queue_manager_id IS NOT NULL
        ) < params.concurrency_limit
    )
      AND EXISTS (
        SELECT 1
        FROM pgqueuer q
        WHERE q.entrypoint = params.entrypoint
          AND q.status = 'queued'
          AND q.execute_after <= NOW()
      )
    LIMIT 1
) sub
