# AGENTS.md -- Guidance for AI Code-Generation Agents

## Project Overview

PgQueuer is a Python library that turns PostgreSQL into a job queue using `LISTEN/NOTIFY` for instant notifications and `FOR UPDATE SKIP LOCKED` for worker coordination. Supports async (asyncpg, psycopg) and sync (psycopg) drivers, with an in-memory adapter for testing. Python >=3.10, async-first, MIT-licensed.

## Project Structure

```
pgqueuer/              Core library (hexagonal architecture)
  domain/              Pure domain: models, types, errors, settings
  ports/               Protocol interfaces (repository, driver, tracing)
  core/                Business logic: QueueManager, SchedulerManager, executors
  adapters/            Infrastructure: DB drivers, persistence, tracing, CLI, in-memory
  *.py (top-level)     Backward-compatibility shims re-exporting from canonical locations
test/                  All tests (unit, integration, windows)
  conftest.py          Shared fixtures (testcontainers-based Postgres, per-test DB)
  helpers.py           Test utilities (mocked_job, wait_until_empty_queue)
  integration/         FastAPI/Flask integration tests
examples/              Consumer, producer, scheduler, framework integration examples
docs/                  MkDocs documentation source
tools/                 Benchmarking and monitoring scripts
```

## Build, Lint, and Test Commands

All commands use `uv` as the package manager. Install deps first: `uv sync --all-extras --frozen`

```bash
# Run ALL checks (recommended before any PR)
make check                  # runs: sync + lint + import-lint + typecheck + pytest

# Individual checks
uv run ruff check .         # Lint (ruff)
uv run ruff format . --check  # Format check
uv run lint-imports         # Hexagonal architecture boundary validation
uv run mypy .               # Type checking (strict, targets Python 3.10)

# Run all tests
uv run pytest

# Run a single test file
uv run pytest test/test_queries.py

# Run a single test function
uv run pytest test/test_queries.py::test_queries_put

# Run a specific parametrized variant
uv run pytest "test/test_queries.py::test_queries_put[1]"

# Run with coverage (as CI does)
uv run pytest --cov=pgqueuer --cov-report=xml --cov-report=term-missing
```

### Database for Tests

Tests use **testcontainers** to auto-start a Postgres container. No manual setup needed if Docker is available. Alternatively, set `EXTERNAL_POSTGRES_DSN` or individual PG env vars:

```bash
PGUSER=pgquser PGDATABASE=pgqdb PGPASSWORD=pgqpw PGHOST=localhost PGPORT=5432
```

To use docker-compose instead: `docker compose up db populate` (and `docker compose down` after).

### Schema Management CLI

```bash
pgq install              # Create tables, triggers, functions
pgq install --dry-run    # Preview SQL without applying
pgq uninstall            # Remove all PgQueuer objects
pgq upgrade              # Apply migrations after a library upgrade
pgq verify --expect present  # Check schema exists (exit 1 on mismatch)
```

### Additional Test Flags

```bash
uv run pytest -vv --log-cli-level=INFO   # Verbose with live log output
uv run pytest -m "not integration"       # Skip integration tests (no DB needed)
```

## Architecture

PgQueuer follows **hexagonal (ports & adapters) architecture** enforced by `import-linter` rules in `pyproject.toml`.

### Layer Structure inside `pgqueuer/`

- **`domain/`** — Pure models, types, settings, errors. No imports from other layers.
- **`ports/`** — Protocol definitions (`Driver`, `RepositoryPort`, `SchemaManagementPort`, etc.). No imports from adapters or core.
- **`core/`** — Business logic:
  - `qm.py` — `QueueManager`: dequeue loop, dispatch, concurrency control, health checks
  - `sm.py` — `SchedulerManager`: cron-based recurring tasks
  - `applications.py` — `PgQueuer`: top-level orchestrator combining QM + SM
  - `executors.py` — `AbstractEntrypointExecutor` / `AbstractScheduleExecutor` and default implementations
  - `buffers.py` — Batched async buffers for heartbeats, job status logs
  - `listeners.py` — PG NOTIFY event routing
  - `tm.py` — `TaskManager` for background task lifecycle
  - `heartbeat.py`, `cache.py`, `helpers.py`, `logconfig.py`
- **`adapters/`** — Concrete implementations:
  - `drivers/` — `asyncpg.py` (AsyncpgDriver, AsyncpgPoolDriver), `psycopg.py` (PsycopgDriver, SyncPsycopgDriver)
  - `persistence/` — `queries.py` (SQL queries), `qb.py` (query builder + DBSettings), `query_helpers.py`
  - `inmemory/` — In-memory driver and queries for testing without Postgres
  - `tracing/` — Logfire, Sentry, OpenTelemetry integrations
  - `cli/` — CLI commands (run, install, dashboard, etc.)

### Import Rules (enforced by lint-imports)

1. **Domain** (`pgqueuer/domain/`) must NOT import from adapters or core
2. **Ports** (`pgqueuer/ports/`) must NOT import from adapters or core (one exception: `ports.driver -> core.tm`)
3. **Core** (`pgqueuer/core/`) must NOT import from adapters (several temporary exceptions listed in `pyproject.toml`)

Port protocols: `QueueRepositoryPort`, `ScheduleRepositoryPort`, `NotificationPort`, `SchemaManagementPort`. The `Queries` class satisfies all four via structural subtyping. Core code should depend on protocols, not `Queries` directly.

### Top-level Shim Modules

Files like `pgqueuer/qm.py`, `pgqueuer/queries.py`, `pgqueuer/models.py`, etc. at the package root are **re-export shims** that import from the layered modules. The public API (`pgqueuer/__init__.py`) exposes `PgQueuer`, `QueueManager`, `SchedulerManager`, `Queries`, `Job`, driver classes, etc.

### Key Patterns

- **Dataclasses** for internal state (`QueueManager`, `Queries`, executors)
- **Pydantic BaseModel** for data transfer objects (`Job`, `Event`, `Schedule`, `Log`)
- **Decorator registration**: `@pgq.entrypoint("name")`, `@pgq.schedule("name", "*/5 * * * *")`
- **Factory classmethods**: `from_asyncpg_connection()`, `from_psycopg_connection()`, `in_memory()`
- **Async-only**: all entrypoints must be async (`async def`); core logic is fully async
- **Buffer pattern**: `JobStatusLogBuffer`, `HeartbeatBuffer` for batched async I/O

### Deprecation Pattern

When deprecating dataclass fields, use a module-level `_SENTINEL = object()` default with a `warnings.warn(..., DeprecationWarning, stacklevel=2)` check in `__post_init__`.

### Dependency Injection

`QueueManager` and `SchedulerManager` accept an optional `queries` argument (default=None). When omitted, they auto-create `Queries(self.connection)`. Prefer injecting a mock for tests.

## Code Style

### Formatting and Linting

- **Formatter/linter**: ruff (line-length=100)
- **Lint rules**: C, E, F, I, PIE, Q, RET, RSE, SIM, W, C90 (max-complexity=15)
- **isort**: combined-as-imports, furthest-to-closest relative imports
- **Mypy** strict mode with Pydantic plugin, targets Python 3.10.

### Import Rules

1. `from __future__ import annotations` (required in every file)
2. Standard library
3. Third-party packages
4. Internal imports using absolute paths (`from pgqueuer.domain.models import Job`)

**No local imports.** All imports must be at module top level. The only exception is `if TYPE_CHECKING:` blocks for avoiding circular imports at runtime.

### Type Annotations

- **Always annotate** all function/method signatures (`mypy: disallow_untyped_defs = true`)
- Use **native Python types**: `list[int]`, `dict[str, Any]`, `int | None` (not `Optional`)
- Use `NewType` for domain primitives: `JobId = NewType("JobId", int)`
- Use `Literal` for string unions: `Literal["queued", "picked", "successful"]`
- Use `Protocol` for structural subtyping (port interfaces)
- Use `TypeAlias` for complex callable types
- **Avoid `Any`** at nearly all cost. The codebase only uses it on driver protocol boundaries for variadic `*args` in SQL query methods -- nowhere else. Use proper types, generics, protocols, or `object` instead.
- **Generic constructors over type-annotated assignments** for typed stdlib objects: `fut = asyncio.Future[MyType]()` not `fut: asyncio.Future[MyType] = asyncio.Future()`.
- **No `# type: ignore` in production code.** `type: ignore` comments are forbidden in `pgqueuer/`. Fix the underlying type issue instead (use `dataclasses.KW_ONLY`, protocols, generics, overloads, etc.). `# type: ignore` is acceptable in test code only.

### Naming Conventions

| Element             | Convention        | Example                          |
|---------------------|-------------------|----------------------------------|
| Classes             | CamelCase         | `QueueManager`, `DuplicateJobError` |
| Functions/methods   | snake_case        | `register_executor`, `fetch_jobs`  |
| Constants/aliases   | UPPER_SNAKE_CASE  | `JOB_STATUS`, `EVENT_TYPES`       |
| Test functions      | `test_` prefix    | `test_queries_put`                 |
| Fixtures            | snake_case        | `apgdriver`, `dsn`                 |

**No leading-underscore prefixes.** Python has no real public/private distinction. Use plain `snake_case` for all methods and attributes — pick a descriptive name instead of hiding it behind a prefix.

### Error Handling

- Custom exceptions inherit from `PgqException` (in `pgqueuer/domain/errors.py`)
- Hierarchy: `PgqException > RetryException > MaxRetriesExceeded | MaxTimeExceeded`; also `DuplicateJobError`, `FailingListenerError`
- Use `logconfig.logger.exception(...)` for logging errors in job dispatch
- Use `contextlib.suppress(...)` for non-critical errors
- Use `pytest.raises` in tests for expected exceptions

### Docstrings

Google-style when present: `Args:`, `Returns:`, `Raises:` sections. Not every method needs one -- omit for trivial/private methods.

### Guiding Principles

- **Follow existing patterns.** Before writing new code, read surrounding modules to match conventions (naming, structure, error handling, dataclass usage, etc.). Do not invent new patterns when an established one exists.
- **Readability and correctness above speed.** Follow the Zen of Python -- explicit is better than implicit, simple is better than complex, readability counts. Never sacrifice clarity or correctness for performance unless there is a measured, proven need.
- **Every change must be proven correct by a test.** Never accept a code change without an accompanying test. Tests must be narrow and precise -- test exactly the behavior being changed, not broad integration sweeps.
- **Every user-facing feature must be documented.** New public APIs, exceptions, executor classes, CLI commands, and behavior changes require corresponding documentation in `docs/`. Update existing guides (e.g., `reliability.md`, `custom-executors.md`, `core-concepts.md`) and create new guide pages when the feature warrants its own topic. Keep the `mkdocs.yml` nav in sync.

## Testing Conventions

- **pytest config**: `asyncio_mode = "auto"`, 20s timeout per test, `--durations=15`
- Declare async tests as `async def test_...` -- no `@pytest.mark.asyncio` decorator needed
- Annotate all test function parameters: `async def test_foo(apgdriver: db.Driver, N: int) -> None:`
- Use `@pytest.mark.parametrize` for multi-value testing
- Fixtures create a **fresh database per test** via `CREATE DATABASE ... TEMPLATE`
- Key fixtures in `test/conftest.py`: `dsn` (per-test DB URL), `apgdriver` (AsyncpgDriver), `pgdriver` (SyncPsycopgDriver)
- In-memory tests (`test_inmemory.py`) don't need Postgres

## Commit Conventions

This project follows [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/).

### Title Format

```
<type>[optional scope]: <description> (#{PR_ID})
```

- **type**: `feat`, `fix`, `docs`, `style`, `refactor`, `perf`, `test`, `build`, `ci`, `chore`
- **scope** (optional): area of the codebase, e.g. `cli`, `qm`, `sm`, `schema`, `drivers`, `inmemory`
- **description**: imperative mood, lowercase, no period at the end
- **`!` after type/scope**: marks a breaking change (e.g. `feat!:` or `feat(cli)!:`)
- **PR reference**: end with the PR number in parentheses, prefixed with `#`

### Examples

```
feat(qm): add global concurrency limit (#92)
fix: restore partial index usage in dequeue CTE (#607)
feat!: simplify CLI connection config, remove dsn() helper (#605)
docs: add custom executor guide (#88)
refactor(core): extract heartbeat into standalone module (#95)
test: add parametrized dequeue batch tests (#100)
chore: bump ruff to 0.8 (#101)
```

### Body and Footer

- Body: present tense, wrap at 72 characters. Explain **why**, not just what.
- Footer: use `BREAKING CHANGE: <description>` for breaking changes (in addition to or instead of `!` in the title).

### Rationale

- `git log --oneline` gives a clean, typed, scannable history.
- `git blame` gives immediate context.
- The `#{PR_ID}` suffix implicitly links to the PR in GitHub.
- Types enable automated changelogs and semantic versioning.

## CI Matrix

Python 3.10--3.14 x Postgres 13--18 on Ubuntu. Windows tests run only `test/windows/test_shutdown.py`.
