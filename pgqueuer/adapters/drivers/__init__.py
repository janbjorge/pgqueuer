"""Database driver adapters and utilities."""

from __future__ import annotations

import os


def dsn(
    host: str = "",
    user: str = "",
    password: str = "",
    database: str = "",
    port: str = "",
) -> str:
    """
    Construct a PostgreSQL DSN (Data Source Name) from parameters or environment variables.

    Assembles a PostgreSQL connection string using provided parameters. If any parameter
    is not specified, it attempts to retrieve it from environment variables (`PGHOST`, `PGUSER`,
    `PGPASSWORD`, `PGDATABASE`, `PGPORT`).

    Returns:
        str: A PostgreSQL DSN string in the format 'postgresql://user:password@host:port/database'.
    """
    host = host or os.getenv("PGHOST", "")
    user = user or os.getenv("PGUSER", "")
    password = password or os.getenv("PGPASSWORD", "")
    database = database or os.getenv("PGDATABASE", "")
    port = port or os.getenv("PGPORT", "")
    return f"postgresql://{user}:{password}@{host}:{port}/{database}"
