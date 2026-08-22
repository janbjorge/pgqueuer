"""Unit tests for the Django adapter. No Postgres, no real Django project.

Covers parameter derivation, entrypoint naming, task discovery and the
sync-to-async wrapper. Django is configured but never connects.
"""

from __future__ import annotations

import json
from typing import Any

import pytest

pytest.importorskip("django")

from test.django_support import configure_django, use_databases  # noqa: E402

configure_django()

from django.tasks import task  # noqa: E402

from pgqueuer.adapters.django.backend import entrypoint_name  # noqa: E402
from pgqueuer.adapters.django.discovery import discover_tasks, tasks_in_module  # noqa: E402
from pgqueuer.adapters.django.driver import worker_connection_params  # noqa: E402
from pgqueuer.adapters.django.executors import build_entrypoint  # noqa: E402
from pgqueuer.domain import models  # noqa: E402


@task()
def sample_task(value: int, offset: int = 0) -> int:
    """A plain sync task, declared at module level as django.tasks requires."""
    return value + offset


def make_job(payload: dict[str, Any] | None) -> models.Job:
    now = models.utc_now()
    return models.Job(
        id=models.JobId(1),
        priority=0,
        created=now,
        updated=now,
        heartbeat=now,
        execute_after=now,
        status="picked",
        entrypoint="queue:sample",
        payload=None if payload is None else json.dumps(payload).encode(),
        queue_manager_id=None,
        headers=None,
    )


def test_worker_connection_params_are_psycopg_ready() -> None:
    databases = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": "derived_db",
            "USER": "someone",
            "HOST": "localhost",
            "PORT": "5432",
            "OPTIONS": {"connect_timeout": 3},
        }
    }
    with use_databases(databases):
        params = worker_connection_params()

    # Django's NAME becomes psycopg's dbname, and OPTIONS pass through.
    assert params["dbname"] == "derived_db"
    assert params["connect_timeout"] == 3
    # Keys psycopg.connect() itself rejects must be gone.
    assert "cursor_factory" not in params
    assert "context" not in params


def test_entrypoint_name_is_queue_prefixed() -> None:
    assert entrypoint_name(sample_task) == f"default:{__name__}.sample_task"


def test_tasks_in_module_finds_declared_tasks() -> None:
    assert sample_task in list(tasks_in_module(__name__))


def test_discover_tasks_keys_by_entrypoint_name() -> None:
    discovered = discover_tasks([__name__])
    assert discovered[f"default:{__name__}.sample_task"] is sample_task


async def test_build_entrypoint_runs_a_sync_callable() -> None:
    seen: list[int] = []

    def handler(value: int, offset: int = 0) -> None:
        seen.append(value + offset)

    entrypoint = build_entrypoint(handler)
    await entrypoint(make_job({"args": [40], "kwargs": {"offset": 2}}))

    assert seen == [42]


async def test_build_entrypoint_tolerates_an_empty_payload() -> None:
    calls: list[int] = []

    def handler() -> None:
        calls.append(1)

    entrypoint = build_entrypoint(handler)
    await entrypoint(make_job(None))

    assert calls == [1]


async def test_build_entrypoint_propagates_handler_errors() -> None:
    def handler() -> None:
        raise ValueError("handler blew up")

    entrypoint = build_entrypoint(handler)
    with pytest.raises(ValueError, match="handler blew up"):
        await entrypoint(make_job({"args": [], "kwargs": {}}))
