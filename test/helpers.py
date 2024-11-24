from __future__ import annotations

import uuid
from datetime import datetime, timezone
from itertools import count

from pgqueuer.models import Job


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
    )
