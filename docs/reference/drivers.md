# Drivers

A driver wraps a PostgreSQL connection and gives PgQueuer one interface for running
queries and receiving notifications, whichever underlying library you use. It also
enforces the configuration queue operations depend on, autocommit in particular.

## Requirements

For any driver:

1. **Autocommit mode**: The connection must operate in autocommit mode.
   - For `psycopg`, explicitly set `connection.autocommit = True`.
   - For `asyncpg`, autocommit is the default unless a transaction is explicitly started.

2. **PostgreSQL compatibility**: The driver must support PostgreSQL-specific features
   used by PgQueuer.

3. **Default isolation level**: Connections should maintain the default PostgreSQL
   isolation level unless explicitly modified.

## Driver protocol

Every driver implements `pgqueuer.ports.driver.Driver`. Built-ins:
`AsyncpgDriver`, `PsycopgDriver`, `SyncPsycopgDriver`, and the in-memory driver. Custom drivers work anywhere a `Driver` is expected.

Key methods:

| Method | Purpose |
|--------|---------|
| `fetch(query, *args)` | Run a SELECT and return rows |
| `execute(query, *args)` | Run a statement and return a status string |
| `add_listener(channel, callback)` | Subscribe to `LISTEN` notifications |
| `notify(channel, payload)` | Send `NOTIFY` on a channel |
| `remove_listener(channel, callback)` | Optional: unsubscribe a callback added with `add_listener` |

`remove_listener` lives on the separate `ListenerRemover` protocol, so existing custom
drivers still satisfy `Driver`. `CompletionWatcher` calls it on exit when a driver has it.

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

## Asynchronous drivers

### `AsyncpgDriver`

Thin wrapper around a single `asyncpg` connection. A connection acquired from
an `asyncpg` pool works too; the driver holds it until you release it.

### `PsycopgDriver`

Built on psycopg's async connection API.

```python
import psycopg
from pgqueuer.db import PsycopgDriver

conn = await psycopg.AsyncConnection.connect(dsn, autocommit=True)
driver = PsycopgDriver(conn)
```

#### psycopg performance

- **Install the C implementation.** `pgqueuer[psycopg]` does not pull in psycopg's C
  implementation, and without it psycopg wraps libpq in pure Python. Install
  `psycopg[binary]` (prebuilt wheels) or `psycopg[c]` (builds against your local libpq).
  Check which one is active with `python -c "import psycopg; print(psycopg.pq.__impl__)"`.
  It should print `binary` or `c`, not `python`.
- **Expect extra latency on quiet queues.** psycopg delivers notifications through
  `notifies()`, which holds the connection lock while it waits. While the listener runs, a
  query on the same connection can wait up to about a second when no `NOTIFY` arrives.
  Under steady load notifications keep arriving, so this costs little. If job latency on
  low-traffic queues matters, use `AsyncpgDriver`.

!!! note "Windows event loop"
    psycopg's async driver is not compatible with Python's default
    `ProactorEventLoop` on Windows. Run your application under
    `SelectorEventLoop` instead. On Python 3.12+ this is a one-liner:

    ```python
    import asyncio
    asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
    ```

    On 3.11 use `asyncio.Runner(loop_factory=asyncio.SelectorEventLoop)`.
    The `pgq` CLI handles this automatically. See the
    [psycopg async docs](https://www.psycopg.org/psycopg3/docs/advanced/async.html).

## Synchronous driver

### `SyncPsycopgDriver`

Designed for blocking code or frameworks such as Flask. **Can only enqueue jobs**: PgQueuer's
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

## Creating PgQueuer instances with classmethods

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

async with asyncpg.create_pool(dsn) as pool, pool.acquire() as connection:
    # PgQueuer holds one connection; jobs borrow from the pool via resources.
    pgq = PgQueuer.from_asyncpg_connection(connection, resources={"pool": pool})
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

## Classmethod parameters

All classmethods accept:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `connection` / `pool` | Yes | The database connection or pool |
| `channel` | No | Custom `Channel` configuration. Defaults to the channel in `settings` |
| `settings` | No | `DBSettings` with table, channel and schema names. Defaults to the `PGQUEUER_*` environment variables |
| `resources` | No | Mutable mapping for shared resources. Defaults to `{}` |

## Recommendations

- Prefer an async driver when your project already runs on asyncio.
- Use the sync driver only to enqueue jobs from short-lived scripts or WSGI applications.
- Reuse connections or pools rather than opening one per enqueue.
- With psycopg, set autocommit before you hand the connection to a driver.

## Troubleshooting

See [Driver Troubleshooting](../development/troubleshooting.md) for a quick-reference
checklist of common driver issues.
