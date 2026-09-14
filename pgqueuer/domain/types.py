from __future__ import annotations

import uuid
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
QueueEntrypoint = NewType("QueueEntrypoint", str)
QueueManagerId = NewType("QueueManagerId", uuid.UUID)
HealthCheckId = NewType("HealthCheckId", uuid.UUID)
Slot = NewType("Slot", int)
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


# Schema objects, named bare: inspect() strips the schema qualifier so a
# declared name compares to an installed one directly.
TableName = NewType("TableName", str)
ColumnName = NewType("ColumnName", str)
IndexName = NewType("IndexName", str)
TypeName = NewType("TypeName", str)
FunctionName = NewType("FunctionName", str)
TriggerName = NewType("TriggerName", str)

# Spellings PostgreSQL itself reports: format_type for one, pg_get_expr for
# the other.
SqlType = NewType("SqlType", str)
SqlExpression = NewType("SqlExpression", str)
