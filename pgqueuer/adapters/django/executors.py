"""Bridge a Django task function into a pgqueuer entrypoint.

A ``django.tasks`` task function is an ordinary synchronous callable, while
pgqueuer entrypoints are coroutines. :func:`build_entrypoint` wraps the former
into the latter via :func:`asgiref.sync.sync_to_async`, which keeps the
``Entrypoint`` type honest and means the stock
:class:`~pgqueuer.core.executors.EntrypointExecutor` can run it unchanged.

Threading policy lives here because it is a Django concern:

``thread_sensitive=True`` (the default) runs every handler on one shared thread,
because database adapters require access from the thread that created them. Jobs
therefore **serialize**, and throughput scales with worker processes rather than
with ``concurrency_limit``.

``thread_sensitive=False`` runs handlers concurrently, but each call gets a fresh
thread, hence a fresh Django connection wrapper and a fresh Postgres session.
Those sessions leak unless closed, so the wrapper always calls
``connections.close_all()`` on the way out.
"""

from __future__ import annotations

import json
from typing import Any, Callable

from asgiref.sync import sync_to_async
from django.db import connections

from pgqueuer.core.executors import AsyncEntrypoint
from pgqueuer.domain import models


def build_entrypoint(
    func: Callable[..., Any],
    *,
    thread_sensitive: bool = True,
    result_table: str | None = None,
) -> AsyncEntrypoint:
    """Wrap the synchronous Django task *func* as an async pgqueuer entrypoint.

    The job payload carries ``args``, ``kwargs`` and ``result_id`` as written by
    :meth:`~pgqueuer.adapters.django.backend.PgqueuerBackend.enqueue`.

    When *result_table* is set, the return value is recorded there keyed by
    ``result_id``. This is a stand-in for real result storage and is expected to
    be replaced by a JSONB column on the job row.
    """

    def call(job: models.Job) -> None:
        payload = json.loads(job.payload) if job.payload else {}
        args = payload.get("args", [])
        kwargs = payload.get("kwargs", {})
        try:
            return_value = func(*args, **kwargs)
            if result_table is not None:
                record_result(result_table, payload.get("result_id"), return_value)
        finally:
            # Only needed when each job got its own thread, and therefore its own
            # connection wrapper, which would otherwise leak a Postgres session.
            # Under thread_sensitive=True one thread is reused, so closing here
            # would just churn the connection on every job.
            if not thread_sensitive:
                connections.close_all()

    async def entrypoint(job: models.Job) -> None:
        await sync_to_async(call, thread_sensitive=thread_sensitive)(job)

    return entrypoint


def record_result(table: str, result_id: str | None, return_value: Any) -> None:
    """Store *return_value* against *result_id*. Table name is caller-controlled."""
    if result_id is None:
        return
    with connections["default"].cursor() as cursor:
        cursor.execute(
            f'UPDATE "{table}" SET status = %s, return_value = %s WHERE result_id = %s',
            ["SUCCESSFUL", json.dumps(return_value), result_id],
        )
