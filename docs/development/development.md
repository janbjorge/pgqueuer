# Contributing

PGQueuer's integration tests use [Testcontainers](https://testcontainers.com/) to launch
an ephemeral PostgreSQL instance automatically. You no longer need to run or manage a local
database manually — just have a container runtime available (Docker Desktop, Colima,
Rancher Desktop, etc.).

## Prerequisites

- Python 3.10+ (managed via `uv` recommended)
- A working Docker (or compatible) daemon on your system path
- Internet access the first time tests pull the PostgreSQL image

## Quick Start

```bash
# 1. Install dependencies (including all extras)
uv sync --all-extras

# 2. Lint and formatting checks
ruff check .

# 3. Type checks
uv run mypy .

# 4. Full test suite (auto-starts and tears down a disposable PostgreSQL)
uv run pytest
```

No manual database bootstrapping required. Schema install happens inside the container
during test setup.

## Makefile (Optional)

```bash
make check
```

Runs lint, type, dependency, and test steps in sequence.

## Test Structure & Tips

- Integration tests trigger the PostgreSQL Testcontainer automatically on first database access.
- The container is reused across tests within a single run for speed, then discarded.

```bash
# More detailed logs
uv run pytest -vv --log-cli-level=INFO

# Skip integration tests
uv run pytest -m "not integration"
```

## Forcing an External Database (Advanced / CI Override)

Provide a full PostgreSQL DSN via `EXTERNAL_POSTGRES_DSN` to bypass Testcontainers:

```bash
export EXTERNAL_POSTGRES_DSN=postgresql://user:pass@localhost:5432/postgres
uv run pytest -v
```

What happens under the hood:

1. The session fixture treats your DSN as a base server reference.
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

## Hot Reloading During Local Development

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

## Development Flow Summary

```bash
uv sync --all-extras
ruff check .
uv run mypy .
uv run pytest -v
```
