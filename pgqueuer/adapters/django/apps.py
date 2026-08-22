"""Django application configuration for the pgqueuer adapter."""

from __future__ import annotations

from django.apps import AppConfig


class PgqueuerConfig(AppConfig):
    """Registers the adapter so ``manage.py pgqworker`` is discoverable.

    ``label`` is a permanent compatibility contract for a reusable app and must
    not change.
    """

    name = "pgqueuer.adapters.django"
    label = "pgqueuer"
    verbose_name = "PgQueuer"
    default_auto_field = "django.db.models.BigAutoField"
