WITH params AS (
SELECT UNNEST($1::text[])       AS expression,
       UNNEST($2::text[])       AS entrypoint,
       UNNEST($3::interval[])   AS delay
)
INSERT INTO pgqueuer_schedules (expression, next_run, entrypoint)
SELECT expression, NOW() + delay, entrypoint FROM params
ON CONFLICT (entrypoint, expression) DO NOTHING
