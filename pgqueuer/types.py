from __future__ import annotations

from typing import Literal, NewType

###### Events ######
PGChannel = NewType("PGChannel", str)
OPERATIONS = Literal["insert", "update", "delete", "truncate"]
STATUS = Literal["queued", "picked"]
EVENT_TYPES = Literal["table_changed_event", "requests_per_second_event", "cancellation_event"]


###### Jobs ######
JobId = NewType("JobId", int)


###### Schedules ######
CronEntrypoint = NewType("CronEntrypoint", str)
CronExpression = NewType("CronExpression", str)
ScheduleId = NewType("ScheduleId", int)
