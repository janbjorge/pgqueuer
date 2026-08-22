"""End-to-end test for examples/django_tracer.py.

Drives the whole path: a sync task enqueued inside ``transaction.atomic()``, a
pgqueuer worker on its own native connection, and the return value read back.

Django's ORM is sync-only, so every Django call here goes through
``asyncio.to_thread``; the worker itself is awaited directly.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import pytest

pytest.importorskip("django")

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from test.django_support import configure_django, use_databases  # noqa: E402

configure_django()

from pgqueuer.domain import types  # noqa: E402

pytestmark = pytest.mark.timeout(90)


def django_databases(dsn: str) -> dict[str, dict[str, Any]]:
    parsed = urlparse(dsn)
    return {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": parsed.path.lstrip("/"),
            "USER": parsed.username or "",
            "PASSWORD": parsed.password or "",
            "HOST": parsed.hostname or "",
            "PORT": str(parsed.port or 5432),
            "OPTIONS": {},
            "CONN_MAX_AGE": 0,
            "TIME_ZONE": None,
            "CONN_HEALTH_CHECKS": False,
            "AUTOCOMMIT": True,
            "ATOMIC_REQUESTS": False,
        }
    }


async def test_sync_task_enqueued_transactionally_and_run_by_worker(dsn: str) -> None:
    from django.core.management import call_command
    from django.db import connections, transaction
    from django.tasks.base import TaskResultStatus

    from examples import django_tracer as tracer

    databases = django_databases(dsn)
    tasks = {
        "default": {
            "BACKEND": f"{tracer.__name__}.TracerBackend",
            "QUEUES": [],
        }
    }

    def setup() -> None:
        # The dsn fixture already installed the pgqueuer schema; contenttypes
        # and the example's result table are ours to create.
        call_command("migrate", "contenttypes", verbosity=0)
        tracer.create_result_table()

    def enqueue_and_roll_back() -> tuple[int, int]:
        """Return the queue depth seen inside the transaction, then after rollback."""
        inside = 0
        try:
            with transaction.atomic():
                tracer.count_content_types.enqueue()
                inside = tracer.queued_count()
                raise RuntimeError("deliberate rollback")
        except RuntimeError:
            pass
        return inside, tracer.queued_count()

    def enqueue_and_commit() -> tuple[str, int]:
        with transaction.atomic():
            result = tracer.count_content_types.enqueue()
        return result.id, tracer.queued_count()

    def read_result(result_id: str) -> tuple[Any, Any]:
        refreshed = tracer.count_content_types.get_result(result_id)
        return refreshed.status, refreshed.return_value

    with use_databases(databases, TASKS=tasks):
        try:
            await asyncio.to_thread(setup)

            # Transactional enqueue: visible inside the block, gone after rollback.
            inside, after_rollback = await asyncio.to_thread(enqueue_and_roll_back)
            assert inside == 1
            assert after_rollback == 0

            result_id, after_commit = await asyncio.to_thread(enqueue_and_commit)
            assert after_commit == 1

            # The worker opens its own native connection; drain exits when empty.
            await tracer.run_worker(
                task_modules=[tracer.__name__],
                result_table=tracer.RESULT_TABLE,
                mode=types.QueueExecutionMode.drain,
            )
            assert await asyncio.to_thread(tracer.queued_count) == 0

            status, return_value = await asyncio.to_thread(read_result, result_id)
            assert status == TaskResultStatus.SUCCESSFUL
            # One ContentType row exists per model in INSTALLED_APPS; the point
            # is that the sync ORM read worked at all, not the exact count.
            assert isinstance(return_value, int)
            assert return_value >= 1
        finally:
            await asyncio.to_thread(connections.close_all)
