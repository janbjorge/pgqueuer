# CLI Reference

The `pgq` CLI provides a command-line interface for managing all aspects of PgQueuer.
It can be invoked as `pgq` or `python3 -m pgqueuer`.

## Commands

### `install`

Set up the necessary database schema for PgQueuer.

**Options:**

- `--durability`: Define the durability level for tables.
  - `volatile`: All tables are unlogged — maximum performance, no crash recovery.
  - `balanced`: Critical tables (`pgqueuer`, `pgqueuer_schedules`) are logged; auxiliary
    tables are unlogged.
  - `durable` *(default)*: All tables are logged — full crash recovery.
- `--dry-run`: Print SQL commands without executing them.

```bash
pgq install --durability balanced
```

---

### `uninstall`

Remove the PgQueuer schema from the database.

**Options:**

- `--dry-run`: Print SQL commands without executing them.

```bash
pgq uninstall
```

---

### `upgrade`

Apply database schema upgrades.

**Options:**

- `--durability`: Adjust the durability level during the upgrade (same options as `install`).
- `--dry-run`: Print SQL commands without executing them.

```bash
pgq upgrade --durability durable
```

---

### `verify`

Ensure PgQueuer tables, triggers, and functions exist (or are absent).

**Options:**

- `--expect` *(required)*: `present` or `absent`.

Prints a message for each missing or unexpected object. Exits with code `1` if any
mismatches are found; `0` otherwise.

```bash
pgq verify --expect present
```

---

### `durability`

Change the durability level of existing PgQueuer tables **without data loss**.

**Arguments:**

- `durability` *(required)*: `volatile`, `balanced`, or `durable`.
- `--dry-run` *(optional)*: Print SQL commands without executing them.

```bash
pgq durability durable
pgq durability volatile --dry-run
```

---

### `autovac`

Apply recommended autovacuum settings for PgQueuer tables.

**Options:**

- `--dry-run`: Print SQL commands without executing them.
- `--rollback`: Reset autovacuum settings to system defaults.

```bash
pgq autovac
pgq autovac --rollback
```

---

### `queue`

Manually enqueue a job.

**Arguments:**

- `entrypoint` *(required)*: The entrypoint name.
- `payload` *(optional)*: A serialized string or JSON payload.

```bash
pgq queue my_module.my_function '{"key": "value"}'
```

---

### `dashboard`

Display a live dashboard showing job statistics.

**Options:**

- `--interval <seconds>`: Refresh interval. If not set, updates once and exits.
- `--tail <number>`: Number of most recent log entries to display.

The table format can be controlled via the `PGQUEUER_TABLEFMT` environment variable
(default: `pretty`).

```bash
pgq dashboard --interval 10 --tail 25
```

---

### `listen`

Listen to PostgreSQL NOTIFY channels for debugging.

**Options:**

- `--channel`: Channel name to listen on (default: `ch_pgqueuer`).

```bash
pgq listen
pgq listen --channel my_custom_channel
```

---

### `schedules`

Manage schedules. Display all schedules or remove specific ones by ID or name.

```bash
pgq schedules
pgq schedules --remove fetch_db
```

---

### `run`

Start a `QueueManager` to process jobs.

**Options:**

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `--dequeue-timeout` | float | 30.0 | Max seconds to wait for new jobs per batch |
| `--batch-size` | int | 10 | Jobs to dequeue per batch |
| `--restart-delay` | float | 5.0 | Seconds between restarts when `--restart-on-failure` is set |
| `--restart-on-failure` | bool | False | Restart the manager automatically after failures |
| `--log-level` | str | INFO | Logging level (DEBUG, INFO, WARNING, ERROR) |
| `--mode` | str | continuous | `continuous` or `drain` |
| `--max-concurrent-tasks` | int | None | Cap total concurrent tasks (None = unlimited) |
| `--shutdown-on-listener-failure` | bool | False | Shut down if the LISTEN channel health check fails |

```bash
# Run with a limit of 5 concurrent tasks
pgq run my_module:my_factory --max-concurrent-tasks 5

# Drain mode — process all queued jobs then exit
pgq run my_module:my_factory --mode drain
```

#### Execution Modes

- **continuous** *(default)*: Keeps processing jobs indefinitely, waiting for new ones.
- **drain**: Processes all available jobs and shuts down once the queue is empty.

Use **continuous** for long-running workers and **drain** for batch processing.

---

## Durability Levels Explained

Durability controls the logging behavior of PgQueuer tables, affecting performance and
crash recovery.

### Volatile

- All tables are **unlogged** — no Write-Ahead Log (WAL) writes.
- Data is **lost** if PostgreSQL crashes.
- Best for: temporary workloads where data loss is acceptable, or maximum throughput testing.

### Balanced

- Critical tables (`pgqueuer`, `pgqueuer_schedules`) are **logged** — survive crashes.
- Auxiliary tables (`pgqueuer_log`, `pgqueuer_statistics`) are **unlogged** — faster writes.
- Best for: production systems where job data must survive crashes but log/statistics can be
  sacrificed for speed.

### Durable *(default)*

- All tables are **logged** — full WAL writes.
- Data survives crashes and restarts.
- Best for: production environments where data integrity is critical.

---

## Factory Pattern (`run` command)

The `run` command uses a factory pattern. Your factory function creates and configures the
manager instance; the CLI loads it, calls it, and runs the returned manager until shutdown.

### Execution Flow

```
pgq run my_module:factory
          │
          ▼
  1. LOAD FACTORY — import module, retrieve function
          │
          ▼
  2. SETUP SIGNAL HANDLERS — SIGINT, SIGTERM
          │
          ▼
  3. SUPERVISOR LOOP — continues until shutdown
          │
          ▼
  4. INVOKE YOUR FACTORY — create connection, register entrypoints
          │
          ▼
  5. LINK SHUTDOWN EVENT — connect signal to manager
          │
          ▼
  6. RUN THE MANAGER
          │
     ┌────┴────┐
     ▼         ▼
  7a. GRACEFUL    7b. RESTART ON FAILURE
      SHUTDOWN        (if --restart-on-failure)
```

### Factory Contract

The factory **must** return an `AsyncContextManager` (typically via `@asynccontextmanager`).
Bare awaitables and sync context managers are **not** accepted — passing one raises
`TypeError` with migration instructions.

```python
from contextlib import asynccontextmanager
import asyncpg
from pgqueuer import PgQueuer

@asynccontextmanager
async def create_pgqueuer():
    conn = await asyncpg.connect()
    pgq = PgQueuer.from_asyncpg_connection(conn)

    @pgq.entrypoint("fetch")
    async def process(job): ...

    yield pgq
```

```bash
pgq run myapp:create_pgqueuer
```

Extra arguments after `--` are forwarded to the factory as `list[str]`:

```bash
pgq run myapp:create_pgqueuer -- --region us-east-1 --workers 4
```

```python
@asynccontextmanager
async def create_pgqueuer(args: list[str]):
    # parse args however you like
    ...
    yield pgq
```

### Key Points

- **Factory runs on each restart**: With `--restart-on-failure`, the factory executes again
  after failures, creating fresh connections and state.
- **Async context manager is required**: Use `@asynccontextmanager` with `yield`.
- **Extra args via `--`**: Arguments after `--` are passed as `list[str]` to the factory.
  Factories that don't need args simply omit the parameter.
- **Shutdown is graceful**: In-flight jobs complete before teardown runs.

See `examples/consumer.py` in the repository for a working example.

---

## Global Options

All commands accept the following connection options:

| Flag | Env Variable | Description |
|------|-------------|-------------|
| `--pg-dsn` | `PGDSN` | Full PostgreSQL connection string (DSN) |
| `--prefix` | `PGQUEUER_PREFIX` | Prefix for PgQueuer database objects |

When `--pg-dsn` is omitted, the database drivers (asyncpg / psycopg) read standard
libpq environment variables automatically: `PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`,
`PGDATABASE`. Use `PGOPTIONS="-csearch_path=myschema"` to set a custom schema.
