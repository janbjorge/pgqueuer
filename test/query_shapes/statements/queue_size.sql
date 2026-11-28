SELECT
    count(*) AS count,
    priority,
    entrypoint,
    status
FROM
    pgqueuer
GROUP BY entrypoint, priority, status
ORDER BY count, entrypoint, priority, status
