# AGENTS.md -- Guidance for AI Code-Generation Agents

## Project Overview

PgQueuer is a Python library using PostgreSQL for job queuing. Python >=3.10, async-first, MIT-licensed.

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

## Code Style

### Formatting and Linting

- **Formatter/linter**: ruff (line-length=100)
- **Lint rules**: C, E, F, I, PIE, Q, RET, RSE, SIM, W, C90 (max-complexity=15)
- **isort**: combined-as-imports, furthest-to-closest relative imports

### Import Ordering

1. `from __future__ import annotations` (required in every file)
2. Standard library
3. Third-party packages
4. Internal imports using absolute paths (`from pgqueuer.domain.models import Job`)

### Type Annotations

- **Always annotate** all function/method signatures (`mypy: disallow_untyped_defs = true`)
- Use **native Python types**: `list[int]`, `dict[str, Any]`, `int | None` (not `Optional`)
- Use `NewType` for domain primitives: `JobId = NewType("JobId", int)`
- Use `Literal` for string unions: `Literal["queued", "picked", "successful"]`
- Use `Protocol` for structural subtyping (port interfaces)
- Use `TypeAlias` for complex callable types
- Target `python_version = "3.10"` in mypy

### Naming Conventions

| Element             | Convention        | Example                          |
|---------------------|-------------------|----------------------------------|
| Classes             | CamelCase         | `QueueManager`, `DuplicateJobError` |
| Functions/methods   | snake_case        | `register_executor`, `fetch_jobs`  |
| Constants/aliases   | UPPER_SNAKE_CASE  | `JOB_STATUS`, `EVENT_TYPES`       |
| Private members     | Leading underscore | `_dispatch`, `_buffer`             |
| Test functions      | `test_` prefix    | `test_queries_put`                 |
| Fixtures            | snake_case        | `apgdriver`, `dsn`                 |

### Error Handling

- Custom exceptions inherit from `PgqException` (in `pgqueuer/domain/errors.py`)
- Hierarchy: `PgqException > RetryException > MaxRetriesExceeded | MaxTimeExceeded`; also `DuplicateJobError`, `FailingListenerError`
- Use `logconfig.logger.exception(...)` for logging errors in job dispatch
- Use `contextlib.suppress(...)` for non-critical errors
- Use `pytest.raises` in tests for expected exceptions

### Docstrings

Google-style when present: `Args:`, `Returns:`, `Raises:` sections. Not every method needs one -- omit for trivial/private methods.

### Key Patterns

- **Dataclasses** for internal state (`QueueManager`, `Queries`, executors)
- **Pydantic BaseModel** for data transfer objects (`Job`, `Event`, `Schedule`, `Log`)
- **Decorator registration**: `@pgq.entrypoint("name")`, `@pgq.schedule("name", "*/5 * * * *")`
- **Factory classmethods**: `from_asyncpg_connection()`, `from_psycopg_connection()`, `in_memory()`
- **Async-only**: all entrypoints must be async (`async def`); core logic is fully async
- **Buffer pattern**: `JobStatusLogBuffer`, `HeartbeatBuffer` for batched async I/O

## Hexagonal Architecture (Ports & Adapters)

The codebase enforces layered boundaries via `import-linter`. Rules:

1. **Domain** (`pgqueuer/domain/`) must NOT import from adapters or core
2. **Ports** (`pgqueuer/ports/`) must NOT import from adapters or core
3. **Core** (`pgqueuer/core/`) must NOT import from adapters (exceptions listed in `pyproject.toml` for migration period)

Port protocols: `QueueRepositoryPort`, `ScheduleRepositoryPort`, `NotificationPort`, `SchemaManagementPort`. The `Queries` class satisfies all four via structural subtyping. Core code should depend on protocols, not `Queries` directly.

### Deprecation Pattern

When deprecating dataclass fields, use a module-level `_SENTINEL = object()` default with a `warnings.warn(..., DeprecationWarning, stacklevel=2)` check in `__post_init__`.

### Dependency Injection

`QueueManager` and `SchedulerManager` accept an optional `queries` argument (default=None). When omitted, they auto-create `Queries(self.connection)`. Prefer injecting a mock for tests.

## Testing Conventions

- **pytest config**: `asyncio_mode = "auto"`, 20s timeout per test, `--durations=15`
- Declare async tests as `async def test_...` -- no `@pytest.mark.asyncio` decorator needed
- Annotate all test function parameters: `async def test_foo(apgdriver: db.Driver, N: int) -> None:`
- Use `@pytest.mark.parametrize` for multi-value testing
- Fixtures create a **fresh database per test** via `CREATE DATABASE ... TEMPLATE`
- In-memory tests (`test_inmemory.py`) don't need Postgres

## Commit Conventions

### Title Format

All commit titles must follow this format:

```
{Title} (#{PR_ID})
```

- **Title**: Imperative mood, starting with a capital letter.
- **PR reference**: End with the PR number in parentheses, prefixed with `#`.

Examples:

```
Add heartbeat buffer flush on shutdown (#87)
Fix duplicate job detection in concurrent workers (#91)
```

### Rationale

- `git log --oneline` gives a clean, readable history.
- `git blame` gives immediate context.
- The `#{PR_ID}` suffix implicitly links to the PR in GitHub.

### Body and Trailers

Present tense, wrap at 72 characters. Reference additional context in the body when needed. See `CONTRIBUTING.md`.

## CI Matrix

Python 3.10--3.14 x Postgres 13--18 on Ubuntu. Windows tests run only `test/windows/test_shutdown.py`.
