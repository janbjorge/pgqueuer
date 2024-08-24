from typing import AsyncGenerator, Generator

import asyncpg
import pytest
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from pgqueuer.db import AsyncpgDriver, Driver
from pgqueuer.queries import Queries


async def get_driver() -> AsyncGenerator[Driver, None]:
    conn = await asyncpg.connect()
    try:
        yield AsyncpgDriver(conn)
    finally:
        await conn.close()


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    app = FastAPI()

    @app.post("/enqueue")
    async def enqueue_job(
        entrypoint: str,
        payload: str,
        priority: int = 0,
        driver: AsyncpgDriver = Depends(get_driver),
    ) -> dict:
        queries = Queries(driver)
        ids = await queries.enqueue(entrypoint, payload.encode(), priority)
        return {"job-ids": ids}

    @app.get("/queue-size")
    async def get_queue_size(
        driver: AsyncpgDriver = Depends(get_driver),
    ) -> dict:
        queries = Queries(driver)
        size = await queries.queue_size()
        return {"queue_size": size}

    with TestClient(app) as client:
        yield client


def test_enqueue_job(client: TestClient) -> None:
    response = client.post(
        "/enqueue",
        params={
            "entrypoint": "test_function",
            "payload": "data",
            "priority": 5,
        },
    )
    assert response.status_code == 200
    assert isinstance(response.json()["job-ids"], list)


def test_queue_size(client: TestClient) -> None:
    response = client.get("/queue-size")
    assert response.status_code == 200
    assert "queue_size" in response.json()
