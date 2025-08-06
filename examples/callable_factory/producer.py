from __future__ import annotations

import os

import psycopg
import uvloop

from pgqueuer.db import PsycopgDriver, dsn
from pgqueuer.queries import Queries


async def main(N: int) -> None:
    connection_string = dsn(
        host=os.environ["POSTGRES_HOST"],
        user=os.environ["POSTGRES_USER"],
        password=os.environ["POSTGRES_PASSWORD"],
        database=os.environ["POSTGRES_DB"],
        port=os.environ["POSTGRES_PORT"],
    )
    conn = await psycopg.AsyncConnection.connect(
        conninfo=connection_string,
        autocommit=True,
    )
    driver = PsycopgDriver(conn)
    queries = Queries(driver)
    await queries.enqueue(
        ["heart_beat"] * N,
        [b""] * N,
        [0] * N,
    )


if __name__ == "__main__":
    uvloop.run(main(10))
