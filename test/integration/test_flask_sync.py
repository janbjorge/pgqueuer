from __future__ import annotations

import sys
from http import HTTPStatus
from pathlib import Path
from typing import Generator
from urllib.parse import urlparse

import pytest
from flask.testing import FlaskClient

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from examples.flask_sync_usage import create_app


@pytest.fixture(scope="function")
def client(monkeypatch: pytest.MonkeyPatch, dsn: str) -> Generator[FlaskClient, None, None]:
    parsed = urlparse(dsn)
    monkeypatch.setenv("PGHOST", parsed.hostname or "")
    monkeypatch.setenv("PGPORT", str(parsed.port or 5432))
    monkeypatch.setenv("PGUSER", parsed.username or "")
    monkeypatch.setenv("PGPASSWORD", parsed.password or "")
    monkeypatch.setenv("PGDATABASE", parsed.path.lstrip("/"))
    with create_app().test_client() as client:
        yield client


def test_enqueue_and_size(client: FlaskClient) -> None:
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
