WITH input AS MATERIALIZED (
    -- MATERIALIZED: nextval is volatile and `input` is referenced twice,
    -- so pin single evaluation -- each input row gets exactly one id.
    -- The scalar sub-select resolves the sequence name once (InitPlan)
    -- rather than a catalog lookup per row.
    -- Skipped ON CONFLICT rows leave harmless id gaps; a pre-count CTE
    -- could avoid them but isn't worth the extra pass.
    SELECT
        nextval(
            (SELECT pg_get_serial_sequence('pgqueuer', 'id'))
        ) AS id,
        priority, entrypoint, payload, execute_after, dedupe_key, headers, ord
    FROM UNNEST(
        $1::int[], $2::text[], $3::bytea[],
        $4::interval[], $5::text[], $6::jsonb[]
    ) WITH ORDINALITY AS
        t(priority, entrypoint, payload, execute_after, dedupe_key, headers, ord)
),
inserted AS (
    INSERT INTO pgqueuer AS job
    (id, priority, entrypoint, payload, execute_after, dedupe_key, headers, status)
    SELECT id, priority, entrypoint, payload, execute_after + NOW(),
        dedupe_key, headers, 'queued'
    FROM input

    RETURNING id, entrypoint, priority, dedupe_key
),
logged AS (
    INSERT INTO pgqueuer_log
    (job_id, status, entrypoint, priority)
    SELECT id, 'queued', entrypoint, priority
    FROM inserted
    -- A job whose priority was updated was queued before; log new jobs only.
    WHERE id IN (SELECT id FROM input)
)
-- Join maps each surviving id back to its input ordinal. Rows skipped by
-- ON CONFLICT are absent from `inserted`, so their positions never appear.
SELECT i.ord AS ord, ins.id AS id
FROM inserted ins
JOIN input i ON i.id = ins.id
ORDER BY i.ord
