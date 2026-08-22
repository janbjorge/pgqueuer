"""``manage.py pgqworker`` — run a pgqueuer worker for this Django project."""

from __future__ import annotations

from argparse import ArgumentParser
from typing import Any

from asgiref.sync import async_to_sync
from django.core.management.base import BaseCommand
from django.db import connections

from pgqueuer.adapters.django.worker import run_worker


class Command(BaseCommand):
    help = "Run a PgQueuer worker for the tasks declared in this project."

    def add_arguments(self, parser: ArgumentParser) -> None:
        parser.add_argument(
            "--database",
            default="default",
            help="Django database alias to derive the worker connection from.",
        )
        parser.add_argument(
            "--tasks-module",
            action="append",
            dest="task_modules",
            help="Module declaring tasks. Repeatable. Defaults to every app's 'tasks' module.",
        )
        parser.add_argument(
            "--queue-name",
            action="append",
            dest="queue_names",
            help="Only serve tasks on this queue. Repeatable. Defaults to all queues.",
        )
        parser.add_argument(
            "--concurrency",
            type=int,
            default=0,
            help="Per-entrypoint concurrency limit; 0 means unlimited.",
        )
        parser.add_argument(
            "--no-thread-sensitive",
            action="store_false",
            dest="thread_sensitive",
            help=(
                "Run handlers on independent threads instead of one shared thread. "
                "Faster, but each handler gets its own database connection."
            ),
        )

    def handle(self, *args: Any, **options: Any) -> None:
        # async_to_sync rather than asyncio.run: it is what enables
        # sync_to_async's thread_sensitive mode below it.
        try:
            async_to_sync(run_worker)(
                alias=options["database"],
                task_modules=options["task_modules"],
                queue_names=options["queue_names"],
                concurrency_limit=options["concurrency"],
                thread_sensitive=options["thread_sensitive"],
            )
        finally:
            # Sync context: close_all() is @async_unsafe and cannot run inside
            # the loop.
            connections.close_all()
