from __future__ import annotations

from typing import Literal, NewType

###### Events ######
Channel = NewType("Channel", str)
PGChannel = Channel  # TODO: Depricate
OPERATIONS = Literal["insert", "update", "delete", "truncate"]
EVENT_TYPES = Literal["table_changed_event", "requests_per_second_event", "cancellation_event"]


###### Jobs ######
JobId = NewType("JobId", int)
STATUS = Literal[
    "queued",
    "picked",
    "successful",
    "exception",
    "canceled",
]


###### Schedules ######
CronEntrypoint = NewType("CronEntrypoint", str)
CronExpression = NewType("CronExpression", str)
ScheduleId = NewType("ScheduleId", int)
