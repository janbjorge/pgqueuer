CREATE OR REPLACE FUNCTION pgq_enqueue(
    queue_table text,
    log_table text,
    priorities int[],
    entrypoints text[],
    payloads bytea[],
    execute_after_intervals interval[],
    dedupe_keys text[],
    headers jsonb[]
)
RETURNS TABLE(id bigint) AS $$
BEGIN
    RETURN QUERY EXECUTE format($f$
        WITH inserted AS (
            INSERT INTO %I
            (priority, entrypoint, payload, execute_after, dedupe_key, headers, status)
            VALUES (
                UNNEST($1::int[]),
                UNNEST($2::text[]),
                UNNEST($3::bytea[]),
                UNNEST($4::interval[]) + NOW(),
                UNNEST($5::text[]),
                UNNEST($6::jsonb[]),
                'queued'
            )
            RETURNING id, entrypoint, status, priority
        )
        INSERT INTO %I
        (job_id, status, entrypoint, priority)
        SELECT id, 'queued', entrypoint, priority
        FROM inserted
        RETURNING job_id AS id
    $f$, queue_table, log_table)
    USING priorities, entrypoints, payloads, execute_after_intervals, dedupe_keys, headers;
END;
$$ LANGUAGE plpgsql VOLATILE;

SELECT * FROM pgq_enqueue(
    'pgqueuer',
    'pgqueuer_log',
    ARRAY[0],
    ARRAY['fetch'],
    ARRAY[convert_to('hello','UTF8')],
    ARRAY[interval '0 seconds'],
    ARRAY[NULL],
    ARRAY['{}'::jsonb]
);
