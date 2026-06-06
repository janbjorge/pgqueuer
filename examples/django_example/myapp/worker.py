"""PGQueuer worker for processing background email jobs.

Run with:
    python -m myapp.worker
"""

import asyncio
import json
import os
import sys

import asyncpg

from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job


async def create_pgqueuer() -> PgQueuer:
    conn = await asyncpg.connect(
        host=os.environ.get("POSTGRES_HOST", "localhost"),
        port=int(os.environ.get("POSTGRES_PORT", 5432)),
        user=os.environ.get("POSTGRES_USER", "postgres"),
        password=os.environ.get("POSTGRES_PASSWORD", "postgres"),
        database=os.environ.get("POSTGRES_DB", "pgqueuer_demo"),
    )
    driver = AsyncpgDriver(conn)
    pgq = PgQueuer(driver)

    @pgq.entrypoint("send_email")
    async def send_email(job: Job) -> None:
        data = json.loads(job.payload)
        recipient = data["recipient"]
        subject = data["subject"]
        print(f"[send_email] Sending to {recipient}: {subject}")

        # Simulate email sending (replace with real SMTP in production)
        await asyncio.sleep(0.5)

        # Update Django model status
        job_db_id = data.get("job_db_id")
        if job_db_id:
            try:
                await conn.execute(
                    "UPDATE myapp_emailjob SET status='completed', updated_at=NOW() WHERE id=$1",
                    job_db_id,
                )
            except Exception as e:
                print(f"[send_email] Warning: could not update Django model: {e}")

        print(f"[send_email] Completed for {recipient}")

    return pgq


async def main() -> None:
    pgq = await create_pgqueuer()
    print("PGQueuer worker started. Listening for jobs...")
    await pgq.run()


if __name__ == "__main__":
    os.environ.setdefault("DJANGO_SETTINGS_MODULE", "myproject.settings")
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    asyncio.run(main())
