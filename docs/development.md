# Development

PGQueuer’s integration tests use Testcontainers to launch an ephemeral PostgreSQL instance automatically. You no longer need to run or manage a local database (or Docker Compose stack) manually—just have a container runtime running (Docker Desktop, Colima, Rancher Desktop, etc.).

## Prerequisites

- Python 3.10+ (managed via `uv` recommended)
- A working Docker (or compatible) daemon available on your system path
- Internet access the first time tests pull the PostgreSQL image

## Quick Start

1. Install dependencies (including extras):
   ```
   uv sync --all-extras
   ```
2. Run lint and formatting checks:
   ```
   ruff check .
   ```
3. Run type checks:
   ```
   uv run mypy .
   ```
4. Run the full test suite (automatically starts & tears down a disposable PostgreSQL):
   ```
   uv run pytest
   ```

That’s it—no manual database bootstrapping required. Migrations / schema install happen inside the container during test setup.

## Makefile (Optional Convenience)

If you prefer a single aggregated check:

- `make check` – Runs lint, type, dependency, and test steps (wrapping the commands shown above).

(Older targets related to Docker Compose are deprecated and can be removed if still present.)

## Test Structure & Tips

- Integration tests will trigger the PostgreSQL Testcontainer automatically on first database access.
- The container is reused across tests within a single run for speed, then discarded.
- If you want to see more detailed logs:
  ```
  uv run pytest -vv --log-cli-level=INFO
  ```
- To run only a subset (e.g., fast/unit tests if they are marked):
  ```
  uv run pytest -m "not integration"
  ```
  (Adjust to whatever markers the project uses—add markers as needed.)

## Forcing an External Database (Advanced / CI Override)

In some scenarios (e.g., benchmarking or debugging against a shared/staging instance) you may want to bypass Testcontainers. The test suite will do this automatically if you provide a full PostgreSQL DSN via the `EXTERNAL_POSTGRES_DSN` environment variable.

Example:
```
export EXTERNAL_POSTGRES_DSN=postgresql://user:pass@localhost:5432/postgres
uv run pytest -v
```

What happens under the hood with an external DSN:

1. The session fixture treats your DSN as a base server reference.
2. It connects to the `postgres` database on that server (the path in the DSN is replaced with `/postgres` temporarily).
3. It creates a temporary template database named `parent_<uuid>`.
4. It installs PGQueuer schema into that template database.
5. For each test function, it creates a fresh database `test_<uuid>` FROM TEMPLATE `parent_<uuid>`, runs the test, then drops it with `DROP DATABASE ... WITH (FORCE)`.
6. At the end of the session the parent template database is discarded automatically (unless the process is interrupted).

Requirements for using `EXTERNAL_POSTGRES_DSN`:
- The user in the DSN must have `CREATE DATABASE` privilege.
- The cluster must have a `postgres` maintenance database (default in standard installations).
- Sufficient permissions to use the template you just created (normal if you created it with the same user).
- Enough disk space / I/O quota for rapid create/drop cycles.

You do NOT need to run `pgq install` manually in this mode; the test harness installs the schema into the template database automatically. The `PGHOST`, `PGUSER`, etc. variables are ignored for the test override unless you incorporate them yourself when constructing the DSN.

If a test run is interrupted (e.g., Ctrl+C) you may see leftover `parent_...` or `test_...` databases. You can safely drop them manually.

Caution: Pointing at a shared production-like server may create load due to frequent database creation. Prefer a dedicated instance for this override.

## Troubleshooting

- Docker not running / cannot connect: Start your Docker daemon and rerun tests.
- Image pull failures: Check network connectivity or corporate proxy settings.
- Stale schema issues when using an external database: Drop the schema or run `pgq uninstall` then `pgq install` to reset.
- Permission errors inside container startup: Ensure your user belongs to the Docker group (Linux) or restart Docker Desktop (macOS/Windows).

## Development Flow Summary

```
uv sync --all-extras
ruff check .
uv run mypy .
uv run pytest -v
```

That loop gives fast feedback with fully isolated, reproducible test runs powered by Testcontainers.

Happy hacking!
