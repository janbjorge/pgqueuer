"""
Django integration example for PGQueuer.

This example shows how to integrate PGQueuer with Django using:
- Django's database connection handling
- Django views for enqueueing jobs
- PGQueuer's async queries for job management

Setup:
1. Add to your Django project's urls.py
2. Configure DATABASES in settings.py to use PostgreSQL
3. Run `pgq install` to set up PGQueuer tables
"""

import json
from typing import Any

from django.http import HttpResponse, JsonResponse
from django.views import View
from django.views.decorators.csrf import csrf_exempt
from django.utils.decorators import method_decorator

from pgqueuer.db import AsyncpgDriver, AsyncpgPoolDriver
from pgqueuer.queries import Queries


class PGQueuerMixin:
    """Mixin to provide PGQueuer queries to Django views."""

    async def get_queries(self) -> Queries:
        """
        Get PGQueuer Queries instance.

        Uses Django's database configuration from settings.DATABASES.
        Expects PostgreSQL to be configured.
        """
        from django.conf import settings
        import asyncpg

        db_config = settings.DATABASES["default"]

        # Create connection pool using Django's database config
        pool = await asyncpg.create_pool(
            host=db_config.get("HOST", "localhost"),
            port=db_config.get("PORT", 5432),
            user=db_config.get("USER", ""),
            password=db_config.get("PASSWORD", ""),
            database=db_config.get("NAME", ""),
        )
        return Queries(AsyncpgPoolDriver(pool))


@method_decorator(csrf_exempt, name="dispatch")
class EnqueueJobView(View, PGQueuerMixin):
    """Django view for enqueueing jobs to PGQueuer."""

    async def post(self, request, *args, **kwargs) -> JsonResponse:
        """
        Enqueue a job to PGQueuer.

        Expected JSON body:
        {
            "entrypoint": "job_name",
            "payload": "job data",
            "priority": 0  // optional
        }
        """
        try:
            data = json.loads(request.body)
            entrypoint = data.get("entrypoint")
            payload = data.get("payload", "")
            priority = data.get("priority", 0)

            if not entrypoint:
                return JsonResponse(
                    {"error": "entrypoint is required"},
                    status=400
                )

            queries = await self.get_queries()
            job_ids = await queries.enqueue(
                entrypoint,
                payload.encode() if isinstance(payload, str) else payload,
                priority
            )

            return JsonResponse({
                "job_ids": job_ids,
                "entrypoint": entrypoint,
                "status": "queued"
            })

        except json.JSONDecodeError:
            return JsonResponse(
                {"error": "Invalid JSON body"},
                status=400
            )
        except Exception as e:
            return JsonResponse(
                {"error": str(e)},
                status=500
            )


class QueueStatsView(View, PGQueuerMixin):
    """Django view for retrieving queue statistics."""

    async def get(self, request, *args, **kwargs) -> JsonResponse:
        """Get current queue size and statistics."""
        try:
            queries = await self.get_queries()
            stats = await queries.queue_size()

            return JsonResponse({
                "queues": [
                    {
                        "entrypoint": s.entrypoint,
                        "priority": s.priority,
                        "status": s.status,
                        "count": s.count,
                    }
                    for s in stats
                ]
            }, safe=False)

        except Exception as e:
            return JsonResponse(
                {"error": str(e)},
                status=500
            )


class ResetPasswordEmailView(View, PGQueuerMixin):
    """Example view: enqueue a password reset email job."""

    async def get(self, request, *args, **kwargs) -> HttpResponse:
        """
        Enqueue a password reset email job.

        Query params:
            user_name: The username to send reset email to
        """
        user_name = request.GET.get("user_name")

        if not user_name:
            return JsonResponse(
                {"error": "user_name query parameter is required"},
                status=400
            )

        try:
            queries = await self.get_queries()
            await queries.enqueue(
                "reset_email_by_user_name",
                payload=json.dumps({"user_name": user_name}).encode(),
            )
            return HttpResponse(status=201)

        except Exception as e:
            return JsonResponse(
                {"error": str(e)},
                status=500
            )


# Django URL configuration example (add to your urls.py):
"""
from django.urls import path
from . import views

urlpatterns = [
    path('pgqueuer/enqueue/', views.EnqueueJobView.as_view(), name='enqueue'),
    path('pgqueuer/stats/', views.QueueStatsView.as_view(), name='queue_stats'),
    path('pgqueuer/reset-password/', views.ResetPasswordEmailView.as_view(), name='reset_password'),
]
"""


# Example Django settings configuration:
"""
DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "NAME": "your_database",
        "USER": "your_user",
        "PASSWORD": "your_password",
        "HOST": "localhost",
        "PORT": "5432",
    }
}
"""


if __name__ == "__main__":
    # This allows running the file directly for testing
    import os
    import django

    # Minimal Django setup for testing
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "__test_settings")

    # Create minimal settings module
    import sys
    test_settings = type(sys)("__test_settings")
    test_settings.DATABASES = {
        "default": {
            "ENGINE": "django.db.backends.postgresql",
            "NAME": "pgqueuer_test",
            "USER": "postgres",
            "PASSWORD": "postgres",
            "HOST": "localhost",
            "PORT": "5432",
        }
    }
    test_settings.SECRET_KEY = "test-secret-key"
    test_settings.USE_TZ = True
    sys.modules["__test_settings"] = test_settings

    django.setup()

    print("Django PGQueuer integration example loaded successfully.")
    print("Add the views to your urls.py to use them.")
