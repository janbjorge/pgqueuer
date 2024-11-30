"""
Models and type definitions for events, jobs, and statistics.

This module defines data classes and types used throughout the application,
including events received from PostgreSQL channels, job representations,
and statistical data structures for logging and monitoring.
"""

from __future__ import annotations

import asyncio
import dataclasses
import uuid
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal, NamedTuple

import anyio
from pydantic import AwareDatetime, BaseModel, Field, RootModel

from .types import (
    EVENT_TYPES,
    OPERATIONS,
    STATUS,
    CronEntrypoint,
    CronExpression,
    JobId,
    PGChannel,
    ScheduleId,
)

###### Events ######


class Event(BaseModel):
    """
    A class representing an event in a PostgreSQL channel.

    Attributes:
        channel: The PostgreSQL channel the event belongs to.
        sent_at: The timestamp when the event was sent.
        type: "table_changed_event" or "requests_per_second_event"
        received_at: The timestamp when the event was received.
    """

    channel: PGChannel
    sent_at: AwareDatetime
    type: EVENT_TYPES
    received_at: AwareDatetime = Field(
        init=False,
        default_factory=lambda: datetime.now(
            tz=timezone.utc,
        ),
    )

    @property
    def latency(self) -> timedelta:
        """
        Calculate the latency between when the event was sent and received.
        """
        return self.received_at - self.sent_at


class TableChangedEvent(Event):
    """
    A class representing an event in a PostgreSQL channel.

    Attributes:
        operation: The type of operation performed (insert, update or delete).
        table: The table the event is associated with.
    """

    type: Literal["table_changed_event"]
    operation: OPERATIONS
    table: str


class RequestsPerSecondEvent(Event):
    """
    A class representing an event in a PostgreSQL channel.

    Attributes:
        entrypoint: The entrypoint to debounce
    """

    type: Literal["requests_per_second_event"]
    entrypoint_count: dict[str, int]


class CancellationEvent(Event):
    """
    A class representing an cancellation event in a PostgreSQL channel.

    Attributes:
        ids: The job-ids to mark for cancellation
    """

    type: Literal["cancellation_event"]
    ids: list[JobId]


class AnyEvent(
    RootModel[
        Annotated[
            TableChangedEvent | RequestsPerSecondEvent | CancellationEvent,
            Field(discriminator="type"),
        ]
    ]
): ...


###### Jobs ######


class Job(BaseModel):
    """
    Represents a job with attributes such as ID, priority,
    creation time, status, entrypoint, and optional payload.
    """

    id: JobId
    priority: int
    created: AwareDatetime
    updated: AwareDatetime
    heartbeat: AwareDatetime
    execute_after: AwareDatetime
    status: STATUS
    entrypoint: str
    payload: bytes | None
    queue_manager_id: uuid.UUID | None


###### Statistics ######
STATUS_LOG = Literal[
    "canceled",
    "exception",
    "successful",
]


class QueueStatistics(BaseModel):
    """
    Represents the number of jobs per entrypoint and priority in the queue.
    """

    count: int
    entrypoint: str
    priority: int
    status: STATUS


class LogStatistics(BaseModel):
    """
    Represents log statistics for jobs based on status, entrypoint, and priority.
    """

    count: int
    created: AwareDatetime
    entrypoint: str
    priority: int
    status: STATUS_LOG
    time_in_queue: timedelta


@dataclasses.dataclass
class Context:
    cancellation: anyio.CancelScope


@dataclasses.dataclass
class EntrypointStatistics:
    samples: deque[tuple[int, datetime]]
    concurrency_limiter: asyncio.Semaphore


###### Schedules ######


class CronExpressionEntrypoint(NamedTuple):
    entrypoint: CronEntrypoint
    expression: CronExpression


class Schedule(BaseModel):
    id: ScheduleId
    expression: CronExpression
    heartbeat: AwareDatetime
    created: AwareDatetime
    updated: AwareDatetime
    next_run: AwareDatetime
    last_run: AwareDatetime | None = None
    status: STATUS
    entrypoint: CronEntrypoint
