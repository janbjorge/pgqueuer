# Installation

## Install the Package

PgQueuer supports two PostgreSQL drivers. Choose the one that fits your stack:

=== "asyncpg (Recommended)"

    ```bash
    pip install pgqueuer[asyncpg]
    ```

    [asyncpg](https://github.com/MagicStack/asyncpg) is a high-performance async driver
    written in Cython. It provides the best throughput for PgQueuer workloads.

=== "psycopg"

    ```bash
    pip install pgqueuer[psycopg]
    ```

    [psycopg](https://www.psycopg.org/psycopg3/) supports both async and synchronous
    connections. Use this if you need sync job handlers or already use psycopg elsewhere
    in your application.

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
| `pgqueuer_status` | Enum | Job status values (`queued`, `picked`, `successful`, `exception`, `canceled`, `deleted`) |
| `pgqueuer_changed` | Function | PL/pgSQL function that sends `pg_notify()` on queue changes |
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

PgQueuer reads standard PostgreSQL environment variables:

| Variable | Purpose | Default |
|----------|---------|---------|
| `PGHOST` | Database host | `localhost` |
| `PGPORT` | Database port | `5432` |
| `PGUSER` | Database user | current OS user |
| `PGPASSWORD` | Database password | — |
| `PGDATABASE` | Database name | same as user |

You can also pass a connection string directly when constructing drivers in code.

## Table Prefix

To run multiple isolated PgQueuer instances in the same database, set a custom prefix:

```bash
PGQUEUER_PREFIX=billing pgq install
PGQUEUER_PREFIX=billing pgq run billing_app:main
```

This prefixes all table names, the enum type, the trigger, and the NOTIFY channel.

## Next Steps

- **[Quick Start](quickstart.md)** -- build your first consumer and producer
- **[Core Concepts](core-concepts.md)** -- understand the mental model behind PgQueuer
