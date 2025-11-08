from __future__ import annotations

import asyncio
import uuid
from collections.abc import Sequence
from datetime import datetime, timezone
from itertools import count
from typing import Protocol

import async_timeout

from pgqueuer.models import Job
from pgqueuer.queries import Queries


class ShutdownCapable(Protocol):
    shutdown: asyncio.Event


def mocked_job(
    id: int | count = count(),
    priority: int = 1,
    created: datetime | None = None,
    heartbeat: datetime | None = None,
    execute_after: datetime | None = None,
    updated: datetime | None = None,
    status: str = "queued",
    entrypoint: str = "test",
    payload: bytes | None = None,
    queue_manager_id: None | uuid.UUID = None,
    headers: dict | None = None,
) -> Job:
    now = datetime.now(timezone.utc)
    return Job(
        id=id if isinstance(id, int) else next(id),
        priority=priority,
        created=created or now,
        heartbeat=heartbeat or now,
        execute_after=execute_after or now,
        updated=updated or now,
        status=status,
        entrypoint=entrypoint,
        payload=payload,
        queue_manager_id=queue_manager_id or uuid.uuid4(),
        headers=headers,
    )


async def wait_until_empty_queue(
    queries: Queries,
    managers: Sequence[ShutdownCapable],
    *,
    timeout_seconds: float = 15.0,
    poll_interval: float = 0.01,
) -> None:
    """
    Block until the queue is empty or the timeout elapses, then signal shutdown.
    """
    async with async_timeout.timeout(timeout_seconds):
        while sum(result.count for result in await queries.queue_size()) > 0:
            await asyncio.sleep(poll_interval)

    for manager in managers:
        manager.shutdown.set()
