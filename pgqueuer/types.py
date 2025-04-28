from __future__ import annotations

from enum import Enum
from typing import Literal, NewType


###### Queue ######
class QueueExecutionMode(Enum):
    continuous = "continuous"  # Normal queue processing with a continuous worker loop
    drain = "drain"  # Process all jobs until empty, then shut down


###### Events ######
Channel = NewType("Channel", str)
PGChannel = Channel  # TODO: Depricate
OPERATIONS = Literal["insert", "update", "delete", "truncate"]
EVENT_TYPES = Literal[
    "table_changed_event",
    "requests_per_second_event",
    "cancellation_event",
    "health_check_event",
]


###### Jobs ######
JobId = NewType("JobId", int)
JOB_STATUS = Literal[
    "queued",
    "picked",
    "successful",
    "canceled",
    "deleted",
    "exception",
]


###### Schedules ######
CronEntrypoint = NewType("CronEntrypoint", str)
CronExpression = NewType("CronExpression", str)
ScheduleId = NewType("ScheduleId", int)
