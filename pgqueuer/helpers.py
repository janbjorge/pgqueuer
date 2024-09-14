from __future__ import annotations

import asyncio
import contextlib
import time
from datetime import datetime, timedelta, timezone

from . import listeners, models


def perf_counter_dt() -> datetime:
    """
    Returns the current high-resolution time as a datetime object.

    This function uses the performance counter (`time.perf_counter()`) for
    the highest available resolution as a timestamp, which is useful for
    time measurements between events.
    """
    return datetime.fromtimestamp(time.perf_counter(), tz=timezone.utc)


def wait_for_notice_event(
    queue: listeners.PGNoticeEventListener,
    timeout: timedelta,
) -> asyncio.Task[models.TableChangedEvent | None]:
    async def suppressed_timeout() -> models.TableChangedEvent | None:
        with contextlib.suppress(asyncio.TimeoutError):
            return await asyncio.wait_for(
                queue.get(),
                timeout=timeout.total_seconds(),
            )
        return None

    return asyncio.create_task(suppressed_timeout())
