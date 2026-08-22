"""A ``django.tasks`` backend that enqueues into pgqueuer.

``django.tasks`` (Django 6.0+) defines the task API but ships no worker; Django's
own documentation delegates execution to the ecosystem. This backend implements
the enqueue half against pgqueuer; ``manage.py pgqworker`` runs the other half.

Enqueue goes through :class:`~pgqueuer.adapters.django.driver.DjangoDriver`, i.e.
Django's own connection, so a job joins the caller's ``transaction.atomic()``
block and disappears with it on rollback.
"""

from __future__ import annotations

import json
from datetime import timedelta
from typing import Any

from django.tasks.backends.base import BaseTaskBackend
from django.tasks.base import Task, TaskResult, TaskResultStatus
from django.tasks.signals import task_enqueued
from django.utils import timezone
from django.utils.crypto import get_random_string

from pgqueuer.adapters.django.driver import DjangoDriver
from pgqueuer.adapters.persistence.queries import SyncQueries

# django.tasks documents result ids as strings shorter than 64 characters.
RESULT_ID_LENGTH = 32


def entrypoint_name(task: Task) -> str:
    """Return the pgqueuer entrypoint for *task*.

    Prefixing with the queue name lets a worker select queues by prefix, while
    one entrypoint per task gives per-task concurrency limits and per-task
    dashboard statistics without extra machinery.
    """
    return f"{task.queue_name}:{task.module_path}"


class PgqueuerBackend(BaseTaskBackend):
    """Enqueue ``django.tasks`` tasks into pgqueuer.

    Recognised ``OPTIONS``:

    ``DATABASE_ALIAS``
        Django database alias to enqueue on. Defaults to ``"default"``.

    Usage example::

        TASKS = {
            "default": {
                "BACKEND": "pgqueuer.adapters.django.backend.PgqueuerBackend",
                "OPTIONS": {"DATABASE_ALIAS": "default"},
            }
        }
    """

    supports_defer = True
    supports_priority = True

    # The worker calls handlers through a thread; coroutine handlers would need
    # a different execution path, so they are rejected up front rather than
    # failing at dispatch.
    supports_async_task = False

    # Result storage is not implemented yet; see the tracer example for a
    # subclass that adds it.
    supports_get_result = False

    def __init__(self, alias: str, params: dict) -> None:
        super().__init__(alias, params)
        self.database_alias = self.options.get("DATABASE_ALIAS", "default")

    def enqueue(
        self,
        task: Task,
        args: list[Any],
        kwargs: dict[str, Any],
    ) -> TaskResult:
        self.validate_task(task)

        task_result = TaskResult(
            task=task,
            id=get_random_string(RESULT_ID_LENGTH),
            status=TaskResultStatus.READY,
            enqueued_at=timezone.now(),
            started_at=None,
            last_attempted_at=None,
            finished_at=None,
            args=args,
            kwargs=kwargs,
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )

        # Read args/kwargs back off the result: TaskResult.__post_init__ has
        # normalised them, which is the serialisation the built-in backends use.
        payload = json.dumps(
            {
                "result_id": task_result.id,
                "args": task_result.args,
                "kwargs": task_result.kwargs,
            }
        ).encode()

        self.pre_enqueue(task_result)
        SyncQueries(DjangoDriver(self.database_alias)).enqueue(
            entrypoint_name(task),
            payload,
            task.priority,
            execute_after=self.execute_after(task),
        )
        task_enqueued.send(type(self), task_result=task_result)
        return task_result

    def pre_enqueue(self, task_result: TaskResult) -> None:
        """Hook for subclasses needing to persist state before the insert."""

    def execute_after(self, task: Task) -> timedelta | None:
        """Translate ``run_after`` into pgqueuer's relative delay."""
        if task.run_after is None:
            return None
        return task.run_after - timezone.now()
