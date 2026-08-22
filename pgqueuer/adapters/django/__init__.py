"""Django integration: a ``django.tasks`` backend and a worker command.

Add the app to ``INSTALLED_APPS`` and point ``TASKS`` at the backend::

    INSTALLED_APPS = [..., "pgqueuer.adapters.django"]
    TASKS = {"default": {"BACKEND": "pgqueuer.adapters.django.backend.PgqueuerBackend"}}

Install the queue schema with ``pgq install``, then run ``manage.py pgqworker``.
"""

from __future__ import annotations

from pgqueuer.adapters.django.backend import PgqueuerBackend, entrypoint_name
from pgqueuer.adapters.django.driver import DjangoDriver, worker_connection_params
from pgqueuer.adapters.django.executors import build_entrypoint

default_app_config = "pgqueuer.adapters.django.apps.PgqueuerConfig"

__all__ = [
    "DjangoDriver",
    "PgqueuerBackend",
    "build_entrypoint",
    "entrypoint_name",
    "worker_connection_params",
]
