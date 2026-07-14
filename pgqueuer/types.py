"""Backward-compatibility shim. Canonical: pgqueuer.domain.types"""

from __future__ import annotations

from pgqueuer.domain.types import (
    EVENT_TYPES,
    JOB_STATUS,
    OPERATIONS,
    Channel,
    CronEntrypoint,
    CronExpression,
    JobId,
    OnConflict,
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
    "OnConflict",
    "OnFailure",
    "QueueExecutionMode",
    "ScheduleId",
]
