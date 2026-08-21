"""Locate declared ``django.tasks`` tasks.

``django.tasks`` keeps no registry of declared tasks — the decorator returns a
:class:`~django.tasks.base.Task` and nothing records it. A worker therefore has
to import the modules that declare tasks and inspect their contents.
"""

from __future__ import annotations

import importlib
from typing import Iterator

from django.apps import apps
from django.tasks.base import Task

from pgqueuer.adapters.django.backend import entrypoint_name


def tasks_in_module(module_path: str) -> Iterator[Task]:
    """Yield every :class:`Task` declared in the module at *module_path*."""
    module = importlib.import_module(module_path)
    for value in vars(module).values():
        if isinstance(value, Task):
            yield value


def default_task_modules() -> Iterator[str]:
    """Yield the ``<app>.tasks`` module path of every installed app that has one."""
    for config in apps.get_app_configs():
        candidate = f"{config.name}.tasks"
        try:
            importlib.import_module(candidate)
        except ModuleNotFoundError:
            continue
        yield candidate


def discover_tasks(module_paths: list[str] | None = None) -> dict[str, Task]:
    """Return tasks keyed by :func:`~pgqueuer.adapters.django.backend.entrypoint_name`.

    With *module_paths* omitted, every installed app's ``tasks`` module is
    scanned. Duplicate keys collapse, which is harmless: the same task imported
    from two modules is still the same task.
    """
    paths = list(default_task_modules()) if module_paths is None else module_paths
    return {entrypoint_name(task): task for path in paths for task in tasks_in_module(path)}
