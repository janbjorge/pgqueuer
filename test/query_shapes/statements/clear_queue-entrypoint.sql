WITH deleted AS (
    DELETE FROM pgqueuer
    WHERE entrypoint = ANY($1)
    RETURNING id, entrypoint, priority
)
INSERT INTO pgqueuer_log (job_id, status, entrypoint, priority)
SELECT id, 'deleted'::pgqueuer_status AS status, entrypoint, priority
FROM deleted
