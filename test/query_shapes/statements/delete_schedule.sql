WITH keep AS (
    SELECT UNNEST($3::text[]) AS entrypoint,
           UNNEST($4::text[]) AS expression
)
DELETE FROM pgqueuer_schedules
WHERE id = ANY($1)
    OR (
        entrypoint = ANY($2)
        AND NOT EXISTS (
            SELECT 1 FROM keep
            WHERE keep.entrypoint = pgqueuer_schedules.entrypoint
            AND keep.expression = pgqueuer_schedules.expression
        )
    )
