# MCP Server

PgQueuer ships an optional [Model Context Protocol (MCP)](https://modelcontextprotocol.io/) server
that gives AI agents read-only access to your queue state, statistics, and schedules.
All queries are predefined — no arbitrary SQL is accepted.

## Installation

```bash
pip install pgqueuer[mcp]
```

This pulls in `mcp>=1.0` and `asyncpg>=0.30.0`.

## Quick Start

The server connects to PostgreSQL using the same
[libpq environment variables](../getting-started/installation.md#connection-configuration)
you already use (`PGHOST`, `PGPORT`, `PGUSER`, `PGPASSWORD`, `PGDATABASE`):

```bash
python -m pgqueuer.adapters.mcp
```

This starts a stdio-based MCP server that any MCP client (Claude Desktop, Claude Code,
Cursor, etc.) can connect to.

### Claude Desktop / Claude Code

Add to your MCP client configuration:

```json
{
  "mcpServers": {
    "pgqueuer": {
      "command": "python",
      "args": ["-m", "pgqueuer.adapters.mcp"],
      "env": {
        "PGHOST": "localhost",
        "PGPORT": "5432",
        "PGUSER": "myuser",
        "PGPASSWORD": "mypassword",
        "PGDATABASE": "mydb"
      }
    }
  }
}
```

### Explicit DSN

If you prefer a connection string over environment variables, use the factory
function directly:

```python
from pgqueuer.adapters.mcp.server import create_mcp_server

server = create_mcp_server(dsn="postgresql://user:pass@host:5432/mydb")
server.run(transport="stdio")
```

### Custom Table Prefix

If you use `PGQUEUER_PREFIX` to namespace your tables, pass custom settings:

```python
from pgqueuer.adapters.mcp.server import create_mcp_server
from pgqueuer.adapters.persistence.qb import DBSettings

server = create_mcp_server(settings=DBSettings())  # reads PGQUEUER_PREFIX
server.run(transport="stdio")
```

## Available Tools

All tools are **read-only** and use predefined static SQL queries.
They cannot modify queue data.

### Queue Overview

| Tool | Description |
|------|-------------|
| `queue_size` | Current job counts grouped by entrypoint, status, and priority. Start here for a quick health check. |
| `queue_table_info` | Browse raw queue rows with pagination. Shows payloads, headers, worker assignments, and timestamps. |

### Throughput & Statistics

| Tool | Description |
|------|-------------|
| `queue_stats` | Per-second time-series of job state transitions. Supports a time-window filter (`period`). |
| `throughput_summary` | High-level totals per entrypoint and status. Easier to read than `queue_stats` when you just want aggregate counts. |

### Failure Investigation

| Tool | Description |
|------|-------------|
| `failed_jobs` | Recent jobs that ended with an exception, including full Python tracebacks as JSON. |

### Worker Health

| Tool | Description |
|------|-------------|
| `active_workers` | Which workers are alive, how many jobs each holds, and their heartbeat timestamps. |
| `stale_jobs` | Jobs stuck in `picked` status with a heartbeat older than a configurable threshold. Indicates dead or hung workers. |
| `queue_age` | Age of the oldest waiting job per entrypoint. Measures backlog depth and whether workers are keeping up. |

### Schedules

| Tool | Description |
|------|-------------|
| `schedules` | All cron-based recurring task definitions with next/last run times and status. |

### Audit & Schema

| Tool | Description |
|------|-------------|
| `queue_log` | Full event log of every job state transition (enqueue, pick, complete, fail, cancel). |
| `schema_info` | PgQueuer table metadata: existence, LOGGED/UNLOGGED durability, disk size, estimated row counts. |

## Programmatic Usage

The `create_mcp_server` factory returns a `FastMCP` instance with no module-level
globals, so you can create multiple servers or embed one in a larger application:

```python
from pgqueuer.adapters.mcp.server import create_mcp_server

# All parameters are optional — asyncpg reads libpq env vars by default
server = create_mcp_server()

# Or pass an explicit DSN and/or custom settings
server = create_mcp_server(dsn="postgresql://...")

server.run(transport="stdio")
```

## Architecture

The MCP server lives in the adapter layer (`pgqueuer/adapters/mcp/`) and follows
the same hexagonal architecture as the rest of PgQueuer:

- **`server.py`** — `create_mcp_server()` factory, `PgQueuerDatabase` wrapper,
  all tool registrations.
- **`__main__.py`** — `python -m pgqueuer.adapters.mcp` entry point.
- All SQL queries are defined as `build_*` methods in
  `pgqueuer/adapters/persistence/qb.py` — no SQL is constructed at runtime.
- The server uses an asyncpg connection pool managed by FastMCP's lifespan.
