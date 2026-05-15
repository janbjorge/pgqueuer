# Drivers

Drivers act as the bridge between PgQueuer and PostgreSQL, managing connections and
abstracting database communication.

## Purpose

Drivers:

- Manage database connections and enforce required configuration (e.g., autocommit).
- Abstract PostgreSQL-specific features to streamline queue operations.
- Provide a consistent interface for executing queries.

## Requirements

For any driver:

1. **Autocommit mode** — The connection must operate in autocommit mode.
   - For `psycopg`, explicitly set `connection.autocommit = True`.
   - For `asyncpg`, autocommit is the default unless a transaction is explicitly started.

2. **PostgreSQL compatibility** — The driver must support PostgreSQL-specific features
   used by PgQueuer.

3. **Default isolation level** — Connections should maintain the default PostgreSQL
   isolation level unless explicitly modified.

## Driver Protocol

Every driver implements `pgqueuer.ports.driver.Driver`. Built-ins:
`AsyncpgDriver`, `AsyncpgPoolDriver`, `PsycopgDriver`, `PsycopgPoolDriver`,
`SyncPsycopgDriver`, and the in-memory driver. Custom drivers work anywhere a
`Driver` is expected.

Key methods:

| Method | Purpose |
|--------|---------|
| `fetch(query, *args)` | Run a SELECT and return rows |
| `execute(query, *args)` | Run a statement and return a status string |
| `add_listener(channel, callback)` | Subscribe to `LISTEN` notifications |
| `notify(channel, payload)` | Send `NOTIFY` on a channel |

`notify()` replaces the removed `build_notify_query()` helper. Each driver sends
`NOTIFY` via its native parameterized API instead of the queries layer building
raw SQL. Custom drivers must implement it.

```python
class Driver(Protocol):
    async def notify(self, channel: str, payload: str) -> None:
        """Send a NOTIFY on *channel* with *payload*."""
        ...
```

Example implementation (matches the built-in `PsycopgDriver`):

```python
async def notify(self, channel: str, payload: str) -> None:
    await self.execute("SELECT pg_notify($1, $2)", channel, payload)
```

## Asynchronous Drivers

### `AsyncpgDriver`

Thin wrapper around a single `asyncpg` connection.

### `AsyncpgPoolDriver`

Uses an `asyncpg` connection pool for improved throughput under high concurrency.

### `PsycopgDriver`

Built on psycopg's async connection API.

```python
import psycopg
from pgqueuer.db import PsycopgDriver

conn = await psycopg.AsyncConnection.connect(dsn)
conn.autocommit = True
driver = PsycopgDriver(conn)
```

!!! note "Windows event loop"
    psycopg's async driver is not compatible with Python's default
    `ProactorEventLoop` on Windows. Run your application under
    `SelectorEventLoop` instead. On Python 3.12+ this is a one-liner:

    ```python
    import asyncio
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
    ```

    On 3.11 use `asyncio.Runner(loop_factory=asyncio.SelectorEventLoop)`;
    on 3.10 set `WindowsSelectorEventLoopPolicy` before `asyncio.run`.
    The `pgq` CLI handles this automatically. See the
    [psycopg async docs](https://www.psycopg.org/psycopg3/docs/advanced/async.html).

## Synchronous Driver

### `SyncPsycopgDriver`

Designed for blocking code or frameworks such as Flask. **Can only enqueue jobs** — PgQueuer's
consumers and internals require an async driver.

```python
import psycopg
from pgqueuer.db import SyncPsycopgDriver
from pgqueuer.queries import SyncQueries

conn = psycopg.connect(dsn, autocommit=True)
driver = SyncPsycopgDriver(conn)
queries = SyncQueries(driver)
queries.enqueue("fetch", b"payload")
```

## Creating PgQueuer Instances with Classmethods

PgQueuer provides classmethods that handle driver instantiation automatically:

### From asyncpg connection

```python
import asyncpg
from pgqueuer import PgQueuer

connection = await asyncpg.connect(dsn)
pgq = PgQueuer.from_asyncpg_connection(connection)

# With optional shared resources
pgq = PgQueuer.from_asyncpg_connection(
    connection,
    resources={"shared_cache": {}},
)

await pgq.run()
```

### From asyncpg pool

```python
import asyncpg
from pgqueuer import PgQueuer

pool = await asyncpg.create_pool(dsn, min_size=2, max_size=10)
pgq = PgQueuer.from_asyncpg_pool(pool)

await pgq.run()
```

### From psycopg async connection

```python
import psycopg
from pgqueuer import PgQueuer

connection = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
pgq = PgQueuer.from_psycopg_connection(connection)

await pgq.run()
```

## Classmethod Parameters

All classmethods accept:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `connection` / `pool` | Yes | The database connection or pool |
| `channel` | No | Custom `Channel` configuration. Defaults to `Channel(DBSettings().channel)` |
| `resources` | No | Mutable mapping for shared resources. Defaults to `{}` |

## Best Practices

- Prefer an async driver when your project already runs on asyncio.
- Use the sync driver only to enqueue jobs from short-lived scripts or WSGI applications.
- Reuse connections or pools; keep autocommit enabled.
- Use the PgQueuer classmethods for simplified setup.
- When using psycopg, always ensure autocommit is enabled before passing the connection.

## Troubleshooting

See [Driver Troubleshooting](../development/troubleshooting.md) for a quick-reference
checklist of common driver issues.
