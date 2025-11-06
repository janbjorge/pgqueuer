# PostgREST Integration

This is a non-trivial database integration.

⚠️⚠️⚠️ Warning: Review your own security, authorization, tenancy, and rate limiting requirements before exposing this RPC.⚠️⚠️⚠️

Do not copy/paste into production without:
- Granting EXECUTE only to the intended role.
- Limiting payload size / content to prevent abuse.
- Validating or restricting acceptable `entrypoint` values.
- Applying appropriate network + authentication controls.
These examples are minimal and need hardening. Table names (`pgqueuer`, `pgqueuer_log`) may differ if you installed PGQueuer with custom settings—adjust everywhere accordingly.

This page describes how to expose PGQueuer job enqueuing through [PostgREST](https://postgrest.org) so any HTTP client can enqueue jobs by calling a PostgreSQL function.

## Overview

The integration provides a single, minimal PostgreSQL function:

- Name: `fn_pgqueuer_enqueue`
- Purpose: Insert one job into the PGQueuer tables and log its initial state.
- Returns: The BIGINT job id.
- Scope: Single-job only (batching intentionally excluded to avoid array length mismatch risks and to keep semantics explicit).
- Security: No implicit `PUBLIC` execute grant. You decide which role(s) can call it.

For high throughput, send concurrent calls or implement a vetted bulk API separately.

## Installation

1. Install the core PGQueuer schema (if not already):
   ```
   pgq install
   ```
2. Create the enqueue function using the SQL below (Manual SQL section).
3. Grant the required privileges (see Required Grants).
4. Configure PostgREST and POST to `/rpc/fn_pgqueuer_enqueue`.

## Manual SQL

Create the function (ADJUST TABLE NAMES IF YOUR INSTALLATION USES DIFFERENT ONES):

```sql
-- Single-job enqueue function for PostgREST
-- NOTE: Replace pgqueuer / pgqueuer_log with your configured table names if they differ.
CREATE OR REPLACE FUNCTION fn_pgqueuer_enqueue(
    entrypoint     TEXT,
    payload        BYTEA    DEFAULT NULL,
    priority       INT      DEFAULT 0,
    execute_after  INTERVAL DEFAULT '0'::INTERVAL,
    dedupe_key     TEXT     DEFAULT NULL,
    headers        JSONB    DEFAULT NULL
) RETURNS BIGINT AS $$
DECLARE
    _job_id BIGINT;
BEGIN
    INSERT INTO pgqueuer
        (priority, entrypoint, payload, execute_after, dedupe_key, headers, status)
    VALUES
        (priority, entrypoint, payload, NOW() + execute_after, dedupe_key, headers, 'queued')
    RETURNING id INTO _job_id;

    INSERT INTO pgqueuer_log
        (job_id, status, entrypoint, priority)
    VALUES (_job_id, 'queued', entrypoint, priority);

    RETURN _job_id;
END;
$$ LANGUAGE plpgsql;
```

## Required Grants (Simplified)

You must explicitly grant:
- EXECUTE on the function
- INSERT / SELECT (and if you need to update job fields, UPDATE) on the queue and log tables
- USAGE + SELECT on the queue table's id sequence

Example (replace `pgqueuer`, `pgqueuer_log`, `pgqueuer_id_seq`, and `pgqapi` if customized):

```sql
GRANT INSERT, SELECT, UPDATE ON TABLE pgqueuer        TO pgqapi;
GRANT INSERT, SELECT, UPDATE ON TABLE pgqueuer_log    TO pgqapi;
GRANT USAGE, SELECT ON SEQUENCE pgqueuer_id_seq       TO pgqapi;
GRANT EXECUTE ON FUNCTION fn_pgqueuer_enqueue(
    TEXT, BYTEA, INT, INTERVAL, TEXT, JSONB
) TO pgqapi;
```

Grant to PUBLIC only if you fully understand the exposure surface (usually avoid).

## PostgREST Configuration Excerpt

```sql
db-uri = "postgres://pgqapi:<password>@localhost:5432/pgqdb"
db-schema = "public"
db-anon-role = "pgqapi"
```

## Enqueue via HTTP

```sql
curl http://localhost:3000/rpc/fn_pgqueuer_enqueue \
  -H "Content-Type: application/json" \
  -d '{
        "entrypoint": "fetch",
        "payload": ""
      }'
```

Optional JSON fields:
- `priority`
- `execute_after` (e.g. "5 minutes")
- `dedupe_key`
- `headers` (object)

Example with optional fields:

```sql
curl http://localhost:3000/rpc/fn_pgqueuer_enqueue \
  -H "Content-Type: application/json" \
  -d '{
        "entrypoint": "fetch",
        "payload": "aGVsbG8=",
        "priority": 5,
        "execute_after": "10 seconds",
        "dedupe_key": "fetch:hello",
        "headers": {"source":"api","version":"1"}
      }'
```

Response body: the numeric job id.

## Security Notes (Condensed)

- REVIEW YOUR OWN SECURITY REQUIREMENTS FIRST (authentication, authorization, rate limiting, audit).
- DO NOT GRANT EXECUTE TO PUBLIC UNLESS YOU FULLY ACCEPT THE RISK.
- LIMIT ACCEPTABLE `entrypoint` VALUES IN YOUR WORKER LOGIC.
- MONITOR / LOG ENQUEUE CALLS.

## Uninstall

```sql
DROP FUNCTION IF EXISTS fn_pgqueuer_enqueue(
    TEXT, BYTEA, INT, INTERVAL, TEXT, JSONB
);
```
