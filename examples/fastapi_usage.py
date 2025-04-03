from typing import AsyncGenerator

import asyncpg
from fastapi import Depends, FastAPI

from pgqueuer.db import AsyncpgDriver, Driver
from pgqueuer.queries import Queries

app = FastAPI()


async def get_driver() -> AsyncGenerator[Driver, None]:
    conn = await asyncpg.connect()
    try:
        yield AsyncpgDriver(conn)
    finally:
        await conn.close()


@app.post("/enqueue")
async def enqueue_job(
    entrypoint: str,
    payload: str,
    priority: int = 0,
    driver: AsyncpgDriver = Depends(get_driver),
) -> dict:
    queries = Queries(driver)
    ids = await queries.enqueue(entrypoint, payload.encode(), priority)
    return {"job_ids": ids}


@app.get("/queue-size")
async def get_queue_size(
    driver: AsyncpgDriver = Depends(get_driver),
) -> list:
    queries = Queries(driver)
    stats = await queries.queue_size()
    return [
        {
            "entrypoint": s.entrypoint,
            "priority": s.priority,
            "status": s.status,
            "count": s.count,
        }
        for s in stats
    ]


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app)
