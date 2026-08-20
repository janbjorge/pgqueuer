# Contributing

PgQueuer's integration tests use [Testcontainers](https://testcontainers.com/) to launch
an ephemeral PostgreSQL instance automatically. You no longer need to run or manage a local
database manually; just have a container runtime available (Docker Desktop, Colima,
Rancher Desktop, etc.).

## Prerequisites

- Python 3.10+ (managed via `uv` recommended)
- A working Docker (or compatible) daemon on your system path
- Internet access the first time tests pull the PostgreSQL image

## Quick start

```bash
# 1. Install dependencies (including all extras)
uv sync --all-extras --frozen

# 2. Lint and formatting checks
uv run ruff check .

# 3. Type checks
uv run mypy .

# 4. Full test suite (auto-starts and tears down a disposable PostgreSQL)
uv run pytest
```

You do not have to bootstrap a database yourself. The test setup installs the schema
inside the container.

## Test structure and tips

- Integration tests trigger the PostgreSQL Testcontainer automatically on first database access.
- The container is reused across tests within a single run for speed, then discarded.

```bash
# More detailed logs
uv run pytest -vv --log-cli-level=INFO

# Skip integration tests
uv run pytest -m "not integration"
```

## Forcing an external database (advanced / CI override)

Provide a full PostgreSQL DSN via `EXTERNAL_POSTGRES_DSN` to bypass Testcontainers:

```bash
export EXTERNAL_POSTGRES_DSN=postgresql://user:pass@localhost:5432/postgres
uv run pytest -v
```

The session fixture then does the following:

1. It treats your DSN as a base server reference.
2. It connects to the `postgres` maintenance database on that server.
3. It creates a temporary template database named `parent_<uuid>` with the PgQueuer schema.
4. For each test, it creates a fresh `test_<uuid>` database `FROM TEMPLATE parent_<uuid>`,
   runs the test, then drops it.
5. At session end, the parent template is discarded.

**Requirements for `EXTERNAL_POSTGRES_DSN`:**

- The user must have `CREATE DATABASE` privilege.
- The cluster must have a `postgres` maintenance database.
- Sufficient disk space for rapid create/drop cycles.

!!! caution
    Pointing at a shared production-like server may create load due to frequent database
    creation. Use a dedicated instance.

## Hot reloading during local development

PgQueuer does not include a built-in `--reload` mode. Use a file-watcher like
[entr](https://eradman.com/entrproject/):

```bash
find . -name '*.py' | entr -rc pgq run main:main
```

This restarts the worker process whenever any Python file changes. For development only.

## Troubleshooting

- **Docker not running**: Start your Docker daemon and rerun tests.
- **Image pull failures**: Check network connectivity or corporate proxy settings.
- **Stale schema (external database)**: Run `pgq uninstall && pgq install` to reset.
- **Permission errors inside container**: Ensure your user is in the Docker group (Linux)
  or restart Docker Desktop (macOS/Windows).

## Development flow summary

```bash
uv sync --all-extras --frozen
uv run ruff check .
uv run lint-imports
uv run mypy .
uv run pytest -v
```
