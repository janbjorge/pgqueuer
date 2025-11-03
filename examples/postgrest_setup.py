"""
PostgREST Integration for PGQueuer

This example demonstrates how to set up a single PostgREST RPC function for
enqueuing one job into PGQueuer. PostgREST exposes PostgreSQL functions as REST
endpoints, enabling HTTP-based job enqueuing.

Rationale: batch (array) enqueue removed to prevent silent NULL padding &
misalignment from multi-UNNEST, keep the example minimal, avoid oversized
payloads, and require explicit per-job intent. For high throughput, issue
multiple calls or implement a dedicated validated bulk API.

To use this integration:

1. Install PGQueuer schema: pgq install
2. Run this script to create the RPC functions: python examples/postgrest_setup.py
3. Configure PostgREST to connect to your database
4. Enqueue jobs via HTTP POST to /rpc/fn_pgqueuer_enqueue with a JSON body
   mapping to function parameters.

Single-job only; batch enqueue support intentionally removed (see rationale above).
"""

import asyncio
import os
from textwrap import dedent

import asyncpg

from pgqueuer.db import dsn
from pgqueuer.qb import QueryBuilderEnvironment


def enqueue_function_sql() -> str:
    """Generate SQL for the single enqueue function (batch removed)."""
    qbe = QueryBuilderEnvironment()

    return dedent(f"""
    -- Single enqueue function (batch support removed)
    -- Rationale: batch arrays removed to avoid UNNEST length mismatch bugs, reduce complexity,
    -- and keep the example focused; use repeated calls or a future bulk API for high volume.
    -- Implementation note: we still reuse the batch INSERT pattern via UNNEST internally
    -- using single-element arrays to stay consistent with library semantics.
    CREATE OR REPLACE FUNCTION fn_pgqueuer_enqueue(
        entrypoint TEXT,
        payload BYTEA DEFAULT NULL,
        priority INT DEFAULT 0,
        execute_after INTERVAL DEFAULT '0'::INTERVAL,
        dedupe_key TEXT DEFAULT NULL,
        headers JSONB DEFAULT NULL
    ) RETURNS BIGINT AS $$
    DECLARE
        _job_id BIGINT;
    BEGIN
        -- Insert a single job
        INSERT INTO {qbe.settings.queue_table}
            (priority, entrypoint, payload, execute_after, dedupe_key, headers, status)
        VALUES
            (priority, entrypoint, payload, NOW() + execute_after, dedupe_key, headers, 'queued')
        RETURNING id INTO _job_id;

        -- Log initial queued status
        INSERT INTO {qbe.settings.queue_table_log}
            (job_id, status, entrypoint, priority)
        VALUES (_job_id, 'queued', entrypoint, priority);

        RETURN _job_id;
    END;
    $$ LANGUAGE plpgsql;

    -- Grant permissions
    GRANT EXECUTE ON FUNCTION fn_pgqueuer_enqueue(
        TEXT, BYTEA, INT, INTERVAL, TEXT, JSONB
    ) TO PUBLIC;

    -- Batch overload & related grant removed; concise single-job RPC only.
    """).strip()


async def main() -> None:
    """Create the single RPC enqueue function in the database."""
    # Get database connection details from environment
    db_dsn = os.environ.get("PGDSN") or dsn(
        database=os.environ.get("PGDATABASE", "pgqdb"),
        password=os.environ.get("PGPASSWORD", "pgqpw"),
        port=os.environ.get("PGPORT", "5432"),
        user=os.environ.get("PGUSER", "pgquser"),
        host=os.environ.get("PGHOST", "localhost"),
    )

    conn = await asyncpg.connect(db_dsn)
    try:
        sql = enqueue_function_sql()
        print("Creating PostgREST RPC enqueue function...")
        print(sql)
        await conn.execute(sql)
        print("✅ PostgREST RPC function created successfully!")
        print("\nTo use with PostgREST:")
        print("1. Configure PostgREST to connect to your database")
        print("2. POST to /rpc/fn_pgqueuer_enqueue with job data")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
