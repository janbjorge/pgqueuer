"""
Setup a second connection with psql and run;
time PGUSER=testuser PGPASSWORD=testpassword PGDATABASE=testdb psql -h 127.0.0.1
testdb=# select pg_notify('debug', 'hello');
testdb=# \\watch 1
"""

from __future__ import annotations

import asyncio
from datetime import datetime

import asyncpg
import psycopg
from PgQueuer.db import AsyncpgDriver, PsycopgDriver


async def main() -> None:
    psy = PsycopgDriver(
        connection=await psycopg.AsyncConnection.connect(
            conninfo="postgresql://testuser:testpassword@localhost:5432/testdb",
            autocommit=True,
        )
    )
    await psy.add_listener(
        "debug",
        lambda event: print(
            datetime.now(),
            "PsycopgDriver:",
            event,
        ),
    )

    apg = AsyncpgDriver(
        connection=await asyncpg.connect(
            dsn="postgresql://testuser:testpassword@localhost:5432/testdb"
        )
    )
    await apg.add_listener(
        "debug",
        lambda event: print(
            datetime.now(),
            "AsyncpgDriver:",
            event,
        ),
    )
    await asyncio.sleep(float("inf"))


if __name__ == "__main__":
    asyncio.run(main())
