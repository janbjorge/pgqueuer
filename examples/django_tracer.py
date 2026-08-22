"""Tracer: a synchronous Django task, enqueued transactionally, run by pgqueuer.

Proves five things in one pass:

1. A plain ``def`` handler runs inside pgqueuer's async worker.
2. ``.enqueue()`` joins the caller's ``transaction.atomic()``: the row is visible
   inside the block and gone after a rollback.
3. The worker runs on its own native psycopg connection, separate from Django's.
4. The handler reads the Django ORM without ``SynchronousOnlyOperation``.
5. The return value is readable afterwards through ``get_result()``.

Not proven here: ``LISTEN/NOTIFY`` dispatch latency. ``drain`` mode picks up
already-queued jobs with its first dequeue, so it exercises the connection split
but not notification-driven wake-up. That needs a long-running worker and a
timing assertion.

Result storage is faked: the return value goes to a table this example creates
rather than to a column on the job row. Everything else is the real path.

Because ``@task`` instantiates the backend at decoration time, Django has to be
configured while this module is still importing — hence ``configure()`` running
at module scope and the ``# noqa: E402`` imports below it. That ordering is
inherent to single-file Django scripts.

Run against a local Postgres::

    PGHOST=localhost PGDATABASE=pgqdb PGUSER=pgquser PGPASSWORD=pgqpw \
        uv run python examples/django_tracer.py
"""

from __future__ import annotations

import asyncio
import json
import os

import django
import psycopg
from django.conf import settings

RESULT_TABLE = "django_tracer_result"


def configure() -> None:
    """Configure Django from PG* environment variables. Idempotent."""
    if settings.configured:
        return
    settings.configure(
        DEBUG=False,
        SECRET_KEY="tracer-only-not-a-secret",
        USE_TZ=True,
        INSTALLED_APPS=[
            "django.contrib.contenttypes",
            "pgqueuer.adapters.django",
        ],
        DATABASES={
            "default": {
                "ENGINE": "django.db.backends.postgresql",
                "NAME": os.environ.get("PGDATABASE", "postgres"),
                "USER": os.environ.get("PGUSER", "postgres"),
                "PASSWORD": os.environ.get("PGPASSWORD", ""),
                "HOST": os.environ.get("PGHOST", "localhost"),
                "PORT": os.environ.get("PGPORT", "5432"),
            }
        },
        TASKS={"default": {"BACKEND": f"{__name__}.TracerBackend", "QUEUES": []}},
    )
    django.setup()


configure()

from django.db import connections, transaction  # noqa: E402
from django.tasks import task  # noqa: E402
from django.tasks.base import TaskResult, TaskResultStatus  # noqa: E402
from django.tasks.exceptions import TaskResultDoesNotExist  # noqa: E402
from django.utils import timezone  # noqa: E402

from pgqueuer.adapters.django.backend import PgqueuerBackend, entrypoint_name  # noqa: E402
from pgqueuer.adapters.django.discovery import discover_tasks  # noqa: E402
from pgqueuer.adapters.django.driver import worker_connection_params  # noqa: E402
from pgqueuer.adapters.django.worker import run_worker  # noqa: E402
from pgqueuer.adapters.drivers.psycopg import PsycopgDriver  # noqa: E402
from pgqueuer.adapters.persistence.queries import Queries  # noqa: E402
from pgqueuer.domain import types  # noqa: E402

# --------------------------------------------------------------------------
# Result storage, faked in the example. In a real integration this is a JSONB
# column on the job row and this subclass disappears.
# --------------------------------------------------------------------------


class TracerBackend(PgqueuerBackend):
    """PgqueuerBackend plus a result table this example owns."""

    supports_get_result = True

    def pre_enqueue(self, task_result: TaskResult) -> None:
        with connections["default"].cursor() as cursor:
            cursor.execute(
                f'INSERT INTO "{RESULT_TABLE}" (result_id, entrypoint, status) VALUES (%s, %s, %s)',
                [task_result.id, entrypoint_name(task_result.task), "READY"],
            )

    def get_result(self, result_id: str) -> TaskResult:
        with connections["default"].cursor() as cursor:
            cursor.execute(
                f'SELECT entrypoint, status, return_value FROM "{RESULT_TABLE}" '
                "WHERE result_id = %s",
                [result_id],
            )
            row = cursor.fetchone()
        if row is None:
            raise TaskResultDoesNotExist(result_id)

        entrypoint, status, return_value = row
        # Django's psycopg backend registers no jsonb loader for raw cursors --
        # it handles JSON at the field level -- so jsonb arrives as text here.
        if return_value is not None:
            return_value = json.loads(return_value)
        result = TaskResult(
            task=discover_tasks([__name__])[entrypoint],
            id=result_id,
            status=TaskResultStatus(status),
            enqueued_at=timezone.now(),
            started_at=None,
            last_attempted_at=None,
            finished_at=None,
            args=[],
            kwargs={},
            backend=self.alias,
            errors=[],
            worker_ids=[],
        )
        object.__setattr__(result, "_return_value", return_value)
        return result


# --------------------------------------------------------------------------
# The task. Module-level and synchronous, exactly as a Django user writes it:
# no async, and a plain ORM query in the body.
# --------------------------------------------------------------------------


@task()
def count_content_types() -> int:
    """Return the number of ContentType rows, read through the sync ORM."""
    from django.contrib.contenttypes.models import ContentType

    return ContentType.objects.count()


# --------------------------------------------------------------------------
# Setup helpers
# --------------------------------------------------------------------------


def create_result_table() -> None:
    with connections["default"].cursor() as cursor:
        cursor.execute(
            f"""CREATE TABLE IF NOT EXISTS "{RESULT_TABLE}" (
                   result_id    text PRIMARY KEY,
                   entrypoint   text NOT NULL,
                   status       text NOT NULL,
                   return_value jsonb
               )"""
        )


async def install_schema() -> None:
    """Install the pgqueuer schema, tolerating an already-installed one."""
    params = worker_connection_params()
    connection = await psycopg.AsyncConnection.connect(**params, autocommit=True)
    try:
        await Queries(PsycopgDriver(connection)).install()
    except psycopg.errors.DuplicateObject:
        pass
    finally:
        await connection.close()


def queued_count() -> int:
    with connections["default"].cursor() as cursor:
        cursor.execute("SELECT count(*) FROM pgqueuer")
        row = cursor.fetchone()
    return int(row[0]) if row else 0


def drain() -> None:
    """Run a worker until the queue is empty, then stop."""
    asyncio.run(
        run_worker(
            task_modules=[__name__],
            result_table=RESULT_TABLE,
            mode=types.QueueExecutionMode.drain,
        )
    )


def main() -> None:
    from django.core.management import call_command

    asyncio.run(install_schema())
    call_command("migrate", "contenttypes", verbosity=0)
    create_result_table()

    print(f"queued at start           : {queued_count()}")

    # --- proof 2a: the job is visible inside the txn, and rolls back with it
    try:
        with transaction.atomic():
            count_content_types.enqueue()
            # Same connection, still inside the transaction: the row is there.
            print(f"queued inside txn         : {queued_count()}   <- expect 1")
            raise RuntimeError("deliberate rollback")
    except RuntimeError:
        pass
    print(f"queued after rollback     : {queued_count()}   <- expect 0")

    # --- proof 2b: commit keeps it ---------------------------------------
    with transaction.atomic():
        result = count_content_types.enqueue()
    print(f"queued after commit       : {queued_count()}   <- expect 1")

    # --- proofs 1, 3, 4: the worker runs the sync handler ------------------
    drain()
    print(f"queued after worker drain : {queued_count()}   <- expect 0")

    # --- proof 5: the return value came back ------------------------------
    refreshed = count_content_types.get_result(result.id)
    print(f"result status             : {refreshed.status}")
    print(f"result return_value       : {refreshed.return_value}")


if __name__ == "__main__":
    main()
