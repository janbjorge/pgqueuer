# Installation

## Install the Package

PgQueuer supports two PostgreSQL drivers. Choose the one that fits your stack:

=== "asyncpg (Recommended)"

    ```bash
    pip install pgqueuer[asyncpg]
    ```

    [asyncpg](https://github.com/MagicStack/asyncpg) is an async PostgreSQL driver
    written in Cython.

=== "psycopg"

    ```bash
    pip install pgqueuer[psycopg]
    ```

    [psycopg](https://www.psycopg.org/psycopg3/) supports both async and synchronous
    connections. Use this if you need to enqueue jobs from sync code (e.g. Flask, Django)
    or already use psycopg elsewhere in your application.

=== "uv"

    ```bash
    uv add pgqueuer[asyncpg]
    ```

!!! tip "Optional extras"

    You can install additional integrations alongside your driver:

    | Extra | Purpose |
    |-------|---------|
    | `asyncpg` | asyncpg async driver |
    | `psycopg` | psycopg async + sync driver |
    | `logfire` | [Logfire](https://logfire.pydantic.dev/) distributed tracing |
    | `sentry` | [Sentry](https://sentry.io/) distributed tracing |
    | `opentelemetry` | [OpenTelemetry](../integrations/tracing.md) distributed tracing |
    | `mcp` | [MCP server](../integrations/mcp-server.md) for AI agent access |
    | `fastapi` | FastAPI Prometheus metrics router |

    Install multiple extras at once:

    ```bash
    pip install pgqueuer[asyncpg,logfire]
    ```

## Set Up the Database Schema

PgQueuer stores jobs, schedules, and logs in PostgreSQL tables. Create them with the CLI:

```bash
pgq install
```

This creates the following objects in your database:

| Object | Type | Purpose |
|--------|------|---------|
| `pgqueuer` | Table | Active job queue |
| `pgqueuer_log` | Table | Completed job audit trail |
| `pgqueuer_statistics` | Table | Job processing statistics |
| `pgqueuer_schedules` | Table | Cron schedule definitions |
| `pgqueuer_status` | Enum | Job status values (`queued`, `picked`, `successful`, `exception`, `canceled`, `deleted`, `failed`) |
| `fn_pgqueuer_changed` | Function | PL/pgSQL function that sends `pg_notify()` on queue changes |
| `tg_pgqueuer_changed` | Trigger | Fires the notify function on INSERT/UPDATE/DELETE/TRUNCATE |

!!! note "Preview before applying"
    Use `--dry-run` to see the SQL without executing it:

    ```bash
    pgq install --dry-run
    ```

## Verify the Installation

Confirm all PgQueuer objects are present:

```bash
pgq verify --expect present
```

This exits with code 0 if the schema is correctly installed, or code 1 if anything is missing.

## Connection Configuration

PgQueuer is bring-your-own-connection: your application opens the connection
or pool with asyncpg/psycopg directly and wraps it in a driver. PgQueuer never
opens, parses, or rewrites anything on that path, so every libpq parameter
(multi-host DSNs, `sslmode`, `target_session_attrs`, `options`, `service=`)
works exactly as your driver supports it.

The drivers read the standard PostgreSQL environment variables:

| Variable | Purpose | Default |
|----------|---------|---------|
| `PGHOST` | Database host | `localhost` |
| `PGPORT` | Database port | `5432` |
| `PGUSER` | Database user | current OS user |
| `PGPASSWORD` | Database password | (none) |
| `PGDATABASE` | Database name | same as user |

### Pool and Connection Tuning

When the `pgq` CLI and the MCP server open their own connection, they read
`PGQUEUER_*` variables. Passing your own factory (`pgq --factory`) skips
this path entirely and hands connection control back to your code. The
opt-in factories in `pgqueuer.adapters.connections` read the same variables
if you want that behavior in your own startup code:

| Variable | Purpose | Default |
|----------|---------|---------|
| `PGQUEUER_DSN` (or `PGDSN`) | Connection string | (libpq env vars) |
| `PGQUEUER_POOL_MIN_SIZE` | Minimum pool connections | `1` |
| `PGQUEUER_POOL_MAX_SIZE` | Maximum pool connections | `5` |
| `PGQUEUER_CONNECT_TIMEOUT` | Connect timeout in seconds | driver default |
| `PGQUEUER_APPLICATION_NAME` | `application_name` shown in `pg_stat_activity` | (unset) |

**These variables have no effect on connections you create yourself.**
Unset variables are never passed to the driver, so DSN parameters and libpq
environment variables always keep control.

The same knobs are available in code through the pool factories:

```python
from pgqueuer.adapters.connections import create_asyncpg_pool, create_psycopg_pool
from pgqueuer.domain.settings import ConnectionSettings

async with create_asyncpg_pool(settings=ConnectionSettings(pool_max_size=10)) as pool:
    ...
```

### libpq Environment Variable Support

psycopg links the real libpq, so every libpq variable works there. asyncpg
reimplements the parsing and supports a subset:

| Variable | asyncpg | psycopg |
|----------|---------|---------|
| `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`, `PGPASSFILE` | yes | yes |
| `PGSSLMODE`, `PGSSLROOTCERT`, `PGSSLCERT`, `PGSSLKEY`, `PGSSLCRL` | yes | yes |
| `PGSSLMINPROTOCOLVERSION`, `PGSSLMAXPROTOCOLVERSION`, `PGSSLNEGOTIATION` | yes | yes |
| `PGTARGETSESSIONATTRS`, `PGKRBSRVNAME`, `PGGSSLIB` | yes | yes |
| `PGCONNECT_TIMEOUT` | via PgQueuer fallback | yes |
| `PGAPPNAME` | no; use `PGQUEUER_APPLICATION_NAME` or a DSN parameter | yes |
| `PGSERVICE` | no; put the settings in a DSN | yes |
| `PGOPTIONS` | no; use a DSN `options=` parameter | yes |

On the asyncpg path PgQueuer fills the `PGCONNECT_TIMEOUT` gap itself:
`PGQUEUER_CONNECT_TIMEOUT` takes precedence, then `PGCONNECT_TIMEOUT`, then
the driver default.

## Table Prefix

To run multiple isolated PgQueuer instances in the same database, set a custom prefix:

```bash
PGQUEUER_PREFIX=billing pgq install
PGQUEUER_PREFIX=billing pgq run billing_app:main
```

This prefixes all table names, the enum type, the trigger, and the NOTIFY channel.

## Next Steps

- **[Quick Start](quickstart.md)**: build your first consumer and producer
- **[Core Concepts](core-concepts.md)**: understand the mental model behind PgQueuer
