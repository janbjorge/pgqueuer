"""Backward-compatibility shim. Canonical: pgqueuer.domain.types"""

from pgqueuer.domain.types import (
    EVENT_TYPES,
    JOB_STATUS,
    OPERATIONS,
    Channel,
    CronEntrypoint,
    CronExpression,
    JobId,
    OnFailure,
    QueueExecutionMode,
    ScheduleId,
)

__all__ = [
    "EVENT_TYPES",
    "JOB_STATUS",
    "OPERATIONS",
    "Channel",
    "CronEntrypoint",
    "CronExpression",
    "JobId",
    "OnFailure",
    "QueueExecutionMode",
    "ScheduleId",
]
