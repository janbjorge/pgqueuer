# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is PgQueuer

PgQueuer is a Python library that turns PostgreSQL into a job queue using `LISTEN/NOTIFY` for instant notifications and `FOR UPDATE SKIP LOCKED` for worker coordination. Supports async (asyncpg, psycopg) and sync (psycopg) drivers, with an in-memory adapter for testing.

## Common Commands

```bash
# Install dependencies
uv sync --all-extras --frozen

# Run all tests (spins up ephemeral Postgres via Testcontainers — Docker must be running)
uv run pytest

# Run a single test
uv run pytest test/test_qm.py::test_name -x

# Lint
uv run ruff check .

# Type check
uv run mypy .

# Import boundary lint (hexagonal architecture enforcement)
uv run lint-imports

# All checks at once
make check
```

## Architecture

PgQueuer follows **hexagonal (ports & adapters) architecture** enforced by `import-linter` rules in `pyproject.toml`.

### Layer structure inside `pgqueuer/`

- **`domain/`** — Pure models, types, settings, errors. No imports from other layers.
- **`ports/`** — Protocol definitions (`Driver`, `RepositoryPort`, `SchemaManagementPort`, etc.). No imports from adapters or core.
- **`core/`** — Business logic:
  - `qm.py` — `QueueManager`: dequeue loop, dispatch, rate limiting, concurrency control, health checks
  - `sm.py` — `SchedulerManager`: cron-based recurring tasks
  - `applications.py` — `PgQueuer`: top-level orchestrator combining QM + SM
  - `executors.py` — `AbstractEntrypointExecutor` / `AbstractScheduleExecutor` and default implementations
  - `buffers.py` — Batched async buffers for heartbeats, job status logs, RPS stats
  - `listeners.py` — PG NOTIFY event routing
  - `tm.py` — `TaskManager` for background task lifecycle
  - `heartbeat.py`, `cache.py`, `helpers.py`, `retries.py`, `logconfig.py`
- **`adapters/`** — Concrete implementations:
  - `drivers/` — `asyncpg.py` (AsyncpgDriver, AsyncpgPoolDriver), `psycopg.py` (PsycopgDriver, SyncPsycopgDriver)
  - `persistence/` — `queries.py` (SQL queries), `qb.py` (query builder + DBSettings), `query_helpers.py`
  - `inmemory/` — In-memory driver and queries for testing without Postgres
  - `tracing/` — Logfire, Sentry, OpenTelemetry integrations
  - `cli/` — CLI commands (run, install, dashboard, etc.)

### Import rules (enforced by lint-imports)

- `domain` must NOT import from `adapters` or `core`
- `ports` must NOT import from `adapters` or `core` (one exception: `ports.driver → core.tm`)
- `core` must NOT import from `adapters` (several temporary exceptions listed in `pyproject.toml`)

### Top-level shim modules

Files like `pgqueuer/qm.py`, `pgqueuer/queries.py`, `pgqueuer/models.py`, etc. at the package root are **re-export shims** that import from the layered modules. The public API (`pgqueuer/__init__.py`) exposes `PgQueuer`, `QueueManager`, `SchedulerManager`, `Queries`, `Job`, driver classes, etc.

## Testing

- Uses **pytest-asyncio** with `asyncio_mode = "auto"` — async test functions run automatically.
- Default test timeout: 20 seconds (per-function via `pytest-timeout`).
- **Testcontainers** launches ephemeral Postgres per session. Set `EXTERNAL_POSTGRES_DSN` to skip container startup.
- Each test function gets its own database (created via `CREATE DATABASE ... TEMPLATE`).
- Key fixtures in `test/conftest.py`: `dsn` (per-test DB URL), `apgdriver` (AsyncpgDriver), `pgdriver` (SyncPsycopgDriver).

## Code Style

- **Ruff** for linting, line length 100.
- **Mypy** strict mode with Pydantic plugin, targets Python 3.10.
- Models use **Pydantic v2** (`BaseModel`, `BaseSettings`).
- Dataclasses for core components (`QueueManager`, `PgQueuer`, `SchedulerManager`).
- **No local imports.** All imports must be at module top level. The only exception is `if TYPE_CHECKING:` blocks for avoiding circular imports at runtime.
- **Avoid `Any`** at nearly all cost. The codebase only uses it on driver protocol boundaries for variadic `*args` in SQL query methods — nowhere else. Use proper types, generics, protocols, or `object` instead.
- **Follow existing patterns.** Before writing new code, read surrounding modules to match conventions (naming, structure, error handling, dataclass usage, etc.). Do not invent new patterns when an established one exists.
- **Readability and correctness above speed.** Follow the Zen of Python — explicit is better than implicit, simple is better than complex, readability counts. Never sacrifice clarity or correctness for performance unless there is a measured, proven need.
- **Every change must be proven correct by a test.** Never accept a code change without an accompanying test. Tests must be narrow and precise — test exactly the behavior being changed, not broad integration sweeps.
- **Every user-facing feature must be documented.** New public APIs, exceptions, executor classes, CLI commands, and behavior changes require corresponding documentation in `docs/`. Update existing guides (e.g., `reliability.md`, `custom-executors.md`, `core-concepts.md`) and create new guide pages when the feature warrants its own topic. Keep the `mkdocs.yml` nav in sync.
