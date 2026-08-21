"""Shared Django bootstrap for the adapter tests.

Django settings are process-global and ``settings.configure()`` may only run
once, so every Django-touching test module goes through :func:`configure_django`
rather than configuring its own. Per-test database changes go through
:func:`use_databases`, which works around the fact that plain
``override_settings(DATABASES=...)`` is silently ineffective.
"""

from __future__ import annotations

import contextlib
from contextlib import contextmanager
from typing import Any, Iterator

PLACEHOLDER_DATABASES: dict[str, dict[str, Any]] = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "placeholder",
        "USER": "placeholder",
        "HOST": "localhost",
        "PORT": "5432",
    }
}


def configure_django() -> None:
    """Configure and initialise Django once per process. Opens no connection."""
    import django
    from django.conf import settings

    if settings.configured:
        return

    settings.configure(
        DEBUG=False,
        SECRET_KEY="test-only-not-a-secret",
        USE_TZ=True,
        INSTALLED_APPS=[
            "django.contrib.contenttypes",
            "pgqueuer.adapters.django",
        ],
        DATABASES=PLACEHOLDER_DATABASES,
        TASKS={
            "default": {
                "BACKEND": "pgqueuer.adapters.django.backend.PgqueuerBackend",
                "QUEUES": [],
            }
        },
    )
    django.setup()


def reset_connection_handler() -> None:
    """Force Django to rebuild connections from the current DATABASES setting.

    ``override_settings(DATABASES=...)`` on its own does nothing once anything
    has touched ``connections``: the handler caches the setting both in a
    ``cached_property`` and in ``_settings``, and ``setting_changed`` invalidates
    neither. That is what Django's "can lead to unexpected behavior" warning
    means in practice. Django's own test runner sidesteps it by building test
    databases before any connection exists.

    Existing connections are *discarded, not closed*: ``close()`` is
    ``@async_unsafe`` and raises if called inside a coroutine. Closing is the
    caller's job, from sync context.
    """
    from django.db import connections

    for alias in list(connections):
        with contextlib.suppress(AttributeError):
            del connections[alias]
    connections._settings = None
    connections.__dict__.pop("settings", None)


@contextmanager
def use_databases(
    databases: dict[str, dict[str, Any]],
    **other_settings: Any,
) -> Iterator[None]:
    """Point Django at *databases* for the duration of the block.

    Resets the connection handler on both entry and exit, so tests are
    independent of each other's ordering.
    """
    from django.test.utils import override_settings

    with override_settings(DATABASES=databases, **other_settings):
        reset_connection_handler()
        try:
            yield
        finally:
            reset_connection_handler()
