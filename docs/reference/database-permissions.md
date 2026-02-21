# Database Permissions

This page lists every PostgreSQL object PgQueuer creates, the privileges needed to install
the schema, and the minimal runtime privileges for the application user.

## Objects Created by `pgq install`

| Object | Name | Type |
|--------|------|------|
| ENUM | `pgqueuer_status` | Custom type |
| Table | `pgqueuer` | Active job queue |
| Table | `pgqueuer_log` | Execution history & audit trail |
| Table | `pgqueuer_statistics` | Aggregated statistics |
| Table | `pgqueuer_schedules` | Recurring schedule definitions |
| Function | `fn_pgqueuer_changed()` | TRIGGER FUNCTION (PL/pgSQL) |
| Trigger | `tg_pgqueuer_changed` | AFTER INSERT/UPDATE/DELETE/TRUNCATE on `pgqueuer` |
| Indexes | (8+) | Priority, heartbeat, dedupe key, log queries |

The trigger calls `pg_notify()` to emit a `table_changed_event` on the configured channel
(`ch_pgqueuer` by default) whenever the queue table changes.

## Installation Privileges

The database role that runs `pgq install` needs:

```sql
-- Required one-time privileges for schema installation
GRANT CREATE ON DATABASE mydb TO pgqueuer_installer;
-- Or, if using a dedicated schema:
GRANT CREATE ON SCHEMA pgqueuer_schema TO pgqueuer_installer;

-- Specific object-level permissions needed:
-- CREATE TYPE   → for pgqueuer_status ENUM
-- CREATE TABLE  → for all 4 tables
-- CREATE INDEX  → for all indexes
-- CREATE FUNCTION → for fn_pgqueuer_changed()
-- CREATE TRIGGER  → for tg_pgqueuer_changed
```

!!! tip "Superuser for install only"
    The simplest approach is to run `pgq install` as a superuser or database owner, then
    grant minimal runtime privileges to the application role separately. The installer role
    does not need to be the role your application runs as.

## Runtime Privileges

The role your application uses at runtime needs significantly fewer permissions:

```sql
-- Create a dedicated application role
CREATE ROLE pgqueuer_app LOGIN PASSWORD 'secret';

-- Schema access
GRANT USAGE ON SCHEMA public TO pgqueuer_app;

-- Active queue: full CRUD needed for enqueue, dequeue, and status updates
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE pgqueuer TO pgqueuer_app;

-- Execution log: insert on completion, select for queries
GRANT SELECT, INSERT ON TABLE pgqueuer_log TO pgqueuer_app;

-- Statistics: aggregation writes
GRANT SELECT, INSERT, UPDATE ON TABLE pgqueuer_statistics TO pgqueuer_app;

-- Schedules: read/write for scheduler
GRANT SELECT, INSERT, UPDATE ON TABLE pgqueuer_schedules TO pgqueuer_app;

-- Trigger function: must be executable by the application user
GRANT EXECUTE ON FUNCTION fn_pgqueuer_changed() TO pgqueuer_app;

-- Sequences (if using serial/bigserial PKs)
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO pgqueuer_app;
```

!!! note "LISTEN/NOTIFY"
    `pg_notify()` is called inside `fn_pgqueuer_changed()`, which runs as the trigger's
    invoker. The application also needs to issue `LISTEN ch_pgqueuer`, which requires no
    special privilege beyond a database connection.

## Recommended: Dedicated Schema

For stronger isolation, install PgQueuer into its own schema:

```sql
CREATE SCHEMA pgq;
GRANT USAGE ON SCHEMA pgq TO pgqueuer_app;

-- Install into the dedicated schema
pgq install --schema pgq
```

This separates PgQueuer objects from application tables and makes privilege management
cleaner — you can grant or revoke schema-level access as a single operation.

## Verifying Permissions

Use `pgq verify` to confirm the schema is correctly installed:

```bash
pgq verify --expect present
```

Exit code `0` means all required objects exist; exit code `1` means something is missing or
extra.

To check that the application role can connect and has the required access:

```sql
-- Run as pgqueuer_app
SELECT has_table_privilege('pgqueuer_app', 'pgqueuer', 'SELECT, INSERT, UPDATE, DELETE');
SELECT has_function_privilege('pgqueuer_app', 'fn_pgqueuer_changed()', 'EXECUTE');
```
