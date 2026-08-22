"""Run a pgqueuer worker over the tasks declared in a Django project.

The worker deliberately does **not** use Django's connection: Django has no
async database layer, so its connection cannot hold the long-lived ``LISTEN``
that pgqueuer's dispatch depends on. It opens its own connection instead, using
parameters derived from Django's configuration.

Connection cleanup is deliberately *not* done here: ``connections.close_all()``
is decorated ``@async_unsafe`` and raises ``SynchronousOnlyOperation`` when
called from a coroutine. Callers close from sync context after the loop exits —
see the ``pgqworker`` management command.
"""

from __future__ import annotations

import psycopg

from pgqueuer.adapters.django.discovery import discover_tasks
from pgqueuer.adapters.django.driver import worker_connection_params
from pgqueuer.adapters.django.executors import build_entrypoint
from pgqueuer.adapters.drivers.psycopg import PsycopgDriver
from pgqueuer.adapters.persistence.queries import Queries
from pgqueuer.core.executors import EntrypointExecutor, EntrypointExecutorParameters
from pgqueuer.core.qm import QueueManager
from pgqueuer.domain import types


async def run_worker(
    *,
    alias: str = "default",
    task_modules: list[str] | None = None,
    queue_names: list[str] | None = None,
    concurrency_limit: int = 0,
    thread_sensitive: bool = True,
    result_table: str | None = None,
    mode: types.QueueExecutionMode = types.QueueExecutionMode.continuous,
) -> None:
    """Register every discovered task as an entrypoint and process jobs.

    *queue_names* filters discovered tasks by queue; omit it to take all of them.
    *mode* accepts ``drain`` to exit once the queue is empty, which is what tests
    want.
    """
    tasks = discover_tasks(task_modules)
    if queue_names is not None:
        allowed = set(queue_names)
        tasks = {name: task for name, task in tasks.items() if task.queue_name in allowed}

    params = worker_connection_params(alias)
    connection = await psycopg.AsyncConnection.connect(**params, autocommit=True)
    try:
        manager = QueueManager(Queries(PsycopgDriver(connection)))
        for entrypoint, task in tasks.items():
            manager.register_executor(
                entrypoint,
                EntrypointExecutor(
                    EntrypointExecutorParameters(
                        concurrency_limit=concurrency_limit,
                        func=build_entrypoint(
                            task.func,
                            thread_sensitive=thread_sensitive,
                            result_table=result_table,
                        ),
                    )
                ),
            )
        await manager.run(mode=mode)
    finally:
        await connection.close()
