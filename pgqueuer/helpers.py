from __future__ import annotations

import time
from datetime import datetime, timezone


def perf_counter_dt() -> datetime:
    """
    Returns the current high-resolution time as a datetime object.

    This function uses the performance counter (`time.perf_counter()`) for
    the highest available resolution as a timestamp, which is useful for
    time measurements between events.
    """
    return datetime.fromtimestamp(time.perf_counter(), tz=timezone.utc)
