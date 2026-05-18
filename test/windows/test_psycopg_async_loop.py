from __future__ import annotations

import os
import sys

import pytest

from pgqueuer.adapters.cli import cli

pytestmark = [
    pytest.mark.skipif(
        sys.platform != "win32",
        reason="ProactorEventLoop is only the default on Windows",
    ),
    pytest.mark.skipif(
        "PGHOST" not in os.environ,
        reason="No PostgreSQL configured (set PGHOST/PGUSER/PGDATABASE to run)",
    ),
]


def _conninfo() -> str:
    from psycopg.conninfo import make_conninfo

    return make_conninfo(
        host=os.environ["PGHOST"],
        port=os.environ.get("PGPORT", "5432"),
        user=os.environ.get("PGUSER", "postgres"),
        password=os.environ.get("PGPASSWORD", ""),
        dbname=os.environ.get("PGDATABASE", "postgres"),
    )


def test_asyncio_run_drives_psycopg_async_connection() -> None:
    psycopg = pytest.importorskip("psycopg")

    conninfo = _conninfo()
    captured: list[int] = []

    async def roundtrip() -> None:
        async with (
            await psycopg.AsyncConnection.connect(conninfo) as conn,
            conn.cursor() as cur,
        ):
            await cur.execute("SELECT 1")
            row = await cur.fetchone()
        assert row is not None
        captured.append(int(row[0]))

    cli.asyncio_run(roundtrip())

    assert captured == [1]
