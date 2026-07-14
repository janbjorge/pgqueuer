from __future__ import annotations

from enum import Enum
from typing import Literal, NewType


class QueueExecutionMode(Enum):
    continuous = "continuous"  # Normal queue processing with a continuous worker loop
    drain = "drain"  # Process all jobs until empty, then shut down


Channel = NewType("Channel", str)
OPERATIONS = Literal["insert", "update", "delete", "truncate"]
EVENT_TYPES = Literal[
    "table_changed_event",
    "cancellation_event",
    "health_check_event",
]


JobId = NewType("JobId", int)
JOB_STATUS = Literal[
    "queued",
    "picked",
    "successful",
    "canceled",
    "deleted",
    "exception",
    "failed",
]

OnConflict = Literal["raise", "skip"]
OnFailure = Literal["delete", "hold"]
SortOrder = Literal["ASC", "DESC"]


CronEntrypoint = NewType("CronEntrypoint", str)
CronExpression = NewType("CronExpression", str)
ScheduleId = NewType("ScheduleId", int)
