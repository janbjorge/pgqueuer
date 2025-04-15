from __future__ import annotations

import sys
from http import HTTPStatus
from pathlib import Path
from typing import Generator

import pytest
from fastapi.testclient import TestClient

from pgqueuer.db import AsyncpgDriver

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from examples.fastapi_usage import create_app


@pytest.fixture
def client() -> Generator[TestClient, None, None]:
    with TestClient(create_app()) as client:
        yield client


async def test_enqueue_and_size(
    client: TestClient,
    apgdriver: AsyncpgDriver,
) -> None:
    # Initial size check
    r1 = client.get("/queue-size")
    assert r1.status_code == HTTPStatus.OK
    assert sum(row["count"] for row in r1.json()) == 0

    # Enqueue job
    params: dict[str, str | int] = {"entrypoint": "sync-test", "payload": "data", "priority": 1}
    r2 = client.post(
        "/enqueue",
        params=params,
    )
    assert r2.status_code == HTTPStatus.OK
    job_ids = r2.json()["job_ids"]
    assert len(job_ids) == 1

    # Check size after enqueue
    r3 = client.get("/queue-size")
    assert r3.status_code == HTTPStatus.OK
    assert any(row["entrypoint"] == "sync-test" and row["count"] == 1 for row in r3.json())
