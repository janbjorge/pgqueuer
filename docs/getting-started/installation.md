# Installation

## Requirements

- Python 3.10+
- PostgreSQL (any recent version)
- An async PostgreSQL driver: `asyncpg` or `psycopg`

## Install

Install PgQueuer with your preferred driver:

=== "asyncpg"

    ```bash
    pip install pgqueuer[asyncpg]
    ```

=== "psycopg"

    ```bash
    pip install pgqueuer[psycopg]
    ```

=== "Both drivers"

    ```bash
    pip install pgqueuer[asyncpg,psycopg]
    ```

=== "uv"

    ```bash
    uv add pgqueuer[asyncpg]
    ```

## Optional extras

| Extra | Purpose |
|-------|---------|
| `asyncpg` | asyncpg async driver |
| `psycopg` | psycopg async + sync driver |
| `logfire` | Logfire distributed tracing |
| `sentry` | Sentry distributed tracing |
| `fastapi` | FastAPI Prometheus metrics router |

## Database setup

After installing, initialize the PgQueuer schema in your PostgreSQL database.
Make sure the standard PostgreSQL environment variables (`PGHOST`, `PGUSER`,
`PGPASSWORD`, `PGDATABASE`) are set, or pass the DSN directly:

```bash
pgq install
```

To verify the installation:

```bash
pgq verify --expect present
```

To remove the schema:

```bash
pgq uninstall
```

See [Database Setup](../reference/database-setup.md) for full details including durability
options and autovacuum tuning.
