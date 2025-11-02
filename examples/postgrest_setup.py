"""
PostgREST Integration for PGQueuer

This example demonstrates how to set up RPC functions for enqueuing jobs
via PostgREST. PostgREST allows you to expose PostgreSQL functions as REST
endpoints, enabling HTTP-based job enqueuing.

To use this integration:

1. Install PGQueuer schema: pgq install
2. Run this script to create the RPC functions: python examples/postgrest_setup.py
3. Configure PostgREST to connect to your database
4. Enqueue jobs via HTTP POST to /rpc/fn_pgqueuer_enqueue

The functions support both single jobs and batches for efficiency.
"""

import asyncio
import os
from textwrap import dedent

from pgqueuer.db import dsn
from pgqueuer.qb import QueryBuilderEnvironment


def get_enqueue_function_sql() -> str:
    """Generate SQL for the overloaded enqueue functions."""
    qbe = QueryBuilderEnvironment()

    return dedent(f"""
    -- Batch enqueue function
    CREATE OR REPLACE FUNCTION {qbe.settings.enqueue_function}(
        entrypoints TEXT[],
        payloads BYTEA[] DEFAULT NULL,
        priorities INT[] DEFAULT ARRAY[0],
        execute_after INTERVAL[] DEFAULT ARRAY['0'::INTERVAL],
        dedupe_keys TEXT[] DEFAULT NULL,
        headers JSONB[] DEFAULT NULL
    ) RETURNS TABLE(id BIGINT) AS $$
    BEGIN
        RETURN QUERY
        WITH inserted AS (
            INSERT INTO {qbe.settings.queue_table}
            (priority, entrypoint, payload, execute_after, dedupe_key, headers, status)
            SELECT
                UNNEST(priorities),
                UNNEST(entrypoints),
                UNNEST(payloads),
                UNNEST(execute_after) + NOW(),
                UNNEST(dedupe_keys),
                UNNEST(headers),
                'queued'
            RETURNING id, entrypoint, status, priority
        )
        INSERT INTO {qbe.settings.queue_table_log}
        (job_id, status, entrypoint, priority)
        SELECT id, 'queued', entrypoint, priority
        FROM inserted
        RETURNING job_id AS id;
    END;
    $$ LANGUAGE plpgsql;

    -- Single enqueue function (wraps batch function)
    CREATE OR REPLACE FUNCTION {qbe.settings.enqueue_function}(
        entrypoint TEXT,
        payload BYTEA DEFAULT NULL,
        priority INT DEFAULT 0,
        execute_after INTERVAL DEFAULT '0'::INTERVAL,
        dedupe_key TEXT DEFAULT NULL,
        headers JSONB DEFAULT NULL
    ) RETURNS BIGINT AS $$
    BEGIN
        RETURN (SELECT id FROM {qbe.settings.enqueue_function}(
            ARRAY[entrypoint],
            ARRAY[payload],
            ARRAY[priority],
            ARRAY[execute_after],
            ARRAY[dedupe_key],
            ARRAY[headers]
        ));
    END;
    $$ LANGUAGE plpgsql;

    -- Grant permissions
    GRANT EXECUTE ON FUNCTION {qbe.settings.enqueue_function}(
        TEXT[], BYTEA[], INT[], INTERVAL[], TEXT[], JSONB[]
    ) TO PUBLIC;

    GRANT EXECUTE ON FUNCTION {qbe.settings.enqueue_function}(
        TEXT, BYTEA, INT, INTERVAL, TEXT, JSONB
    ) TO PUBLIC;
    """).strip()


async def main() -> None:
    """Create the RPC functions in the database."""
    # Get database connection details from environment
    db_dsn = os.environ.get("PGDSN") or dsn(
        database=os.environ.get("PGDATABASE", "pgqdb"),
        password=os.environ.get("PGPASSWORD", "pgqpw"),
        port=os.environ.get("PGPORT", "5432"),
        user=os.environ.get("PGUSER", "pgquser"),
        host=os.environ.get("PGHOST", "localhost"),
    )

    # Import here to avoid circular imports
    import asyncpg

    from pgqueuer.db import AsyncpgDriver

    # Connect and execute
    conn = await asyncpg.connect(db_dsn)
    driver = AsyncpgDriver(conn)

    try:
        sql = get_enqueue_function_sql()
        print("Creating PostgREST RPC functions...")
        print(sql)
        await driver.execute(sql)
        print("✅ PostgREST RPC functions created successfully!")
        print("\nTo use with PostgREST:")
        print("1. Configure PostgREST to connect to your database")
        print("2. POST to /rpc/fn_pgqueuer_enqueue with job data")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
