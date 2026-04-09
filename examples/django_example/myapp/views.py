"""Django views for enqueueing and monitoring PGQueuer background jobs."""

import asyncio
import json

from django.http import HttpRequest, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

from pgqueuer.db import AsyncpgDriver
from pgqueuer.queries import Queries

from .models import EmailJob


async def _get_queries() -> Queries:
    """Create a PGQueuer Queries instance using Django's DB config."""
    import asyncpg
    from django.conf import settings

    db = settings.DATABASES["default"]
    pool = await asyncpg.create_pool(
        host=db.get("HOST", "localhost"),
        port=int(db.get("PORT", 5432)),
        user=db.get("USER", ""),
        password=db.get("PASSWORD", ""),
        database=db.get("NAME", ""),
    )
    return Queries(AsyncpgDriver(pool)), pool


def index(request: HttpRequest):
    """Landing page with links to enqueue and monitor jobs."""
    return render(request, "myapp/index.html")


@csrf_exempt
@require_POST
def enqueue_email(request: HttpRequest):
    """Enqueue a background email task via PGQueuer."""
    recipient = request.POST.get("recipient", "")
    subject = request.POST.get("subject", "Test Email")
    body = request.POST.get("body", "This is a test email from PGQueuer + Django.")

    if not recipient:
        return JsonResponse({"error": "recipient is required"}, status=400)

    # Create DB record
    job = EmailJob.objects.create(
        recipient=recipient, subject=subject, body=body, status="pending"
    )

    # Enqueue with PGQueuer
    async def _enqueue():
        queries, pool = await _get_queries()
        try:
            jid = await queries.enqueue(
                "send_email",
                json.dumps(
                    {
                        "job_db_id": job.id,
                        "recipient": recipient,
                        "subject": subject,
                        "body": body,
                    }
                ).encode(),
            )
            job.pgqueuer_job_id = jid
            job.status = "processing"
            job.save(update_fields=["pgqueuer_job_id", "status"])
            return jid
        finally:
            await pool.close()

    jid = asyncio.run(_enqueue())

    return JsonResponse(
        {"message": "Email job enqueued", "job_id": job.id, "pgqueuer_job_id": jid}
    )


def job_status(request: HttpRequest):
    """Show recent email jobs and their status."""
    jobs = EmailJob.objects.all()[:50]
    return render(request, "myapp/job_status.html", {"jobs": jobs})


async def queue_stats(request: HttpRequest):
    """Return PGQueuer queue statistics as JSON."""
    queries, pool = await _get_queries()
    try:
        stats = await queries.queue_size()
        return JsonResponse({"queue_stats": stats})
    finally:
        await pool.close()
