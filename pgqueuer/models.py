"""Backward-compatibility shim. Canonical: pgqueuer.domain.models"""

from __future__ import annotations

from pgqueuer.domain.models import (
    AnyEvent,
    CancellationEvent,
    Context,
    CronExpressionEntrypoint,
    Event,
    HealthCheckEvent,
    Job,
    Log,
    LogStatistics,
    QueueStatistics,
    ResourceKey,
    Schedule,
    ScheduleContext,
    TableChangedEvent,
    TracebackRecord,
)
from pgqueuer.domain.types import (
    EVENT_TYPES,
    JOB_STATUS,
    OPERATIONS,
    Channel,
    CronEntrypoint,
    CronExpression,
    JobId,
    ScheduleId,
)

__all__ = [
    "AnyEvent",
    "CancellationEvent",
    "Channel",
    "Context",
    "CronEntrypoint",
    "CronExpression",
    "CronExpressionEntrypoint",
    "EVENT_TYPES",
    "Event",
    "HealthCheckEvent",
    "JOB_STATUS",
    "Job",
    "JobId",
    "Log",
    "LogStatistics",
    "OPERATIONS",
    "QueueStatistics",
    "ResourceKey",
    "Schedule",
    "ScheduleContext",
    "ScheduleId",
    "TableChangedEvent",
    "TracebackRecord",
]
