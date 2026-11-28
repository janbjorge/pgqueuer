SELECT
    c.relname AS table_name,
    CASE WHEN c.relpersistence = 'u' THEN 'UNLOGGED'
         WHEN c.relpersistence = 'p' THEN 'LOGGED'
         ELSE c.relpersistence::text
    END AS persistence,
    c.reltuples::bigint AS estimated_rows,
    pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = current_schema()
  AND c.relname IN (
    'pgqueuer',
    'pgqueuer_log',
    'pgqueuer_statistics',
    'pgqueuer_schedules'
  )
  AND c.relkind = 'r'
ORDER BY c.relname
