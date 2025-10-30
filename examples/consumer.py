from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from typing import AsyncGenerator

import asyncpg

from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.logconfig import logger
from pgqueuer.models import Job


async def create_pgqueuer() -> PgQueuer:
    """Create and configure a PgQueuer instance with job handlers."""
    print("🔌 Connecting to Postgres...")
    connection = await asyncpg.connect()
    driver = AsyncpgDriver(connection)
    pgq = PgQueuer(driver)

    # Counter to track processed jobs in this session
    processed_count = {"count": 0}

    # Setup the 'send_email' entrypoint handler
    @pgq.entrypoint("send_email")
    async def send_email(job: Job) -> None:
        """Process an email job."""
        payload = json.loads((job.payload or b"").decode())
        processed_count["count"] += 1

        recipient = payload.get("recipient", "unknown")
        subject = payload.get("subject", "no subject")

        # Simulate sending email (in real code, call your email service here)
        print(f"  📧 Email #{processed_count['count']}: {recipient}")
        print(f"     Subject: {subject}")
        print(f"     Job ID: {job.id}")

        # Small delay to simulate real work
        await asyncio.sleep(0.01)

    return pgq


@asynccontextmanager
async def main() -> AsyncGenerator[PgQueuer, None]:
    """
    Context manager for setting up and tearing down the PgQueuer instance.

    This ensures proper initialization and cleanup of database connections
    and job handlers when the consumer is running.

    Yields:
        PgQueuer: The configured consumer instance.
    """
    logger.info("🚀 Starting PgQueuer consumer...")
    try:
        pgq = await create_pgqueuer()
        print("📡 Consumer ready, listening for jobs...")
        print("-" * 50)
        yield pgq
    finally:
        logger.info("🛑 Shutting down consumer...")
        print("-" * 50)
        print("✅ Consumer stopped")


if __name__ == "__main__":
    import uvloop

    async def run() -> None:
        async with main() as pgq:
            # Run the consumer - it will process jobs continuously
            # Press Ctrl+C to stop
            await pgq.run(batch_size=10)

    print("=" * 50)
    print("PgQueuer Consumer Example")
    print("=" * 50)
    print("💡 Press Ctrl+C to stop\n")

    try:
        uvloop.run(run())
    except KeyboardInterrupt:
        print("\n👋 Consumer interrupted by user")
