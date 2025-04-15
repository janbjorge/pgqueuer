from __future__ import annotations

import sys
from http import HTTPStatus
from pathlib import Path
from typing import Generator

import pytest
from flask.testing import FlaskClient

from pgqueuer.db import SyncPsycopgDriver

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from examples.flask_sync_usage import create_app


@pytest.fixture(scope="module")
def client() -> Generator[FlaskClient, None, None]:
    with create_app().test_client() as client:
        yield client


def test_enqueue_and_size(
    client: FlaskClient,
    pgdriver: SyncPsycopgDriver,
) -> None:
    # Initial size check
    r1 = client.get("/queue_size")
    assert r1.status_code == HTTPStatus.OK
    assert isinstance(r1.json, list)
    assert sum(row["count"] for row in r1.json) == 0

    # Enqueue job
    payload = {"entrypoint": "sync-test", "payload": "data", "priority": 1}
    r2 = client.post("/enqueue", json=payload)
    assert r2.status_code == HTTPStatus.OK
    assert isinstance(r2.json, dict)
    job_ids = r2.json["job_ids"]
    assert len(job_ids) == 1

    # Check size after enqueue
    r3 = client.get("/queue_size")
    assert r3.status_code == HTTPStatus.OK
    assert isinstance(r3.json, list)
    assert any(row["entrypoint"] == "sync-test" and row["count"] == 1 for row in r3.json)
