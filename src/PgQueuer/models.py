from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal, NewType

from pydantic import AwareDatetime, BaseModel, Field, RootModel

###### Events ######

PGChannel = NewType(
    "PGChannel",
    str,
)


OPERATIONS = Literal[
    "insert",
    "update",
    "delete",
    "truncate",
]


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
    type: Literal["table_changed_event", "requests_per_second_event"]
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
    entrypoint: str
    count: int


class AnyEvent(
    RootModel[
        Annotated[
            TableChangedEvent | RequestsPerSecondEvent,
            Field(discriminator="type"),
        ]
    ]
): ...


###### Jobs ######
JobId = NewType(
    "JobId",
    int,
)

STATUS = Literal[
    "queued",
    "picked",
]


class Job(BaseModel):
    """
    Represents a job with attributes such as ID, priority,
    creation time, status, entrypoint, and optional payload.
    """

    id: JobId
    priority: int
    created: AwareDatetime
    status: STATUS
    entrypoint: str
    payload: bytes | None


###### Statistics ######
STATUS_LOG = Literal[
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
