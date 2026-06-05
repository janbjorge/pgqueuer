from __future__ import annotations

import dataclasses
import traceback
import uuid
from collections.abc import MutableMapping
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any, Generic, Literal, NamedTuple, TypeVar

import anyio
from pydantic import AwareDatetime, BaseModel, BeforeValidator, Field, RootModel
from pydantic_core import from_json

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


def utc_now() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(timezone.utc)


class Event(BaseModel):
    """Base NOTIFY event. ``received_at`` is stamped on validation."""

    channel: Channel
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
        """``received_at - sent_at``."""
        return self.received_at - self.sent_at


class TableChangedEvent(Event):
    """Row-level change on the queue table."""

    type: Literal["table_changed_event"]
    operation: OPERATIONS
    table: str


class CancellationEvent(Event):
    """Request cancellation of *ids*."""

    type: Literal["cancellation_event"]
    ids: list[JobId]


class HealthCheckEvent(Event):
    """Echo response for a health-check probe; ``id`` matches the originating probe."""

    id: uuid.UUID
    type: Literal["health_check_event"]


class AnyEvent(
    RootModel[
        Annotated[
            TableChangedEvent | CancellationEvent | HealthCheckEvent,
            Field(discriminator="type"),
        ]
    ]
): ...


class Job(BaseModel):
    """A queued or in-flight job row."""

    id: JobId
    priority: int
    created: AwareDatetime
    updated: AwareDatetime
    heartbeat: AwareDatetime
    execute_after: AwareDatetime
    status: JOB_STATUS
    entrypoint: str
    payload: bytes | None
    attempts: int = 0
    queue_manager_id: uuid.UUID | None
    headers: Annotated[
        dict[str, Any] | None,
        BeforeValidator(lambda x: None if x is None else from_json(x)),
    ]

    def logfire_headers(self) -> dict[str, Any] | None:
        """Return the ``logfire`` sub-dict from job headers, or None."""
        return None if self.headers is None else self.headers.get("logfire")

    def sentry_headers(self) -> dict[str, Any] | None:
        """Return the ``sentry`` sub-dict from job headers, or None."""
        return None if self.headers is None else self.headers.get("sentry")

    def otel_headers(self) -> dict[str, Any] | None:
        """Return the ``otel`` W3C propagation sub-dict from job headers, or None."""
        return None if self.headers is None else self.headers.get("otel")


class Log(BaseModel):
    """Represents a job status log entry recording a state transition."""

    created: AwareDatetime
    job_id: JobId
    status: JOB_STATUS
    priority: int
    entrypoint: str
    traceback: Annotated[
        TracebackRecord | None,
        BeforeValidator(lambda x: None if x is None else from_json(x)),
    ]
    aggregated: bool


class QueueStatistics(BaseModel):
    """Per-(entrypoint, priority, status) job count snapshot."""

    count: int
    entrypoint: str
    priority: int
    status: JOB_STATUS


class LogStatistics(BaseModel):
    """Per-(entrypoint, priority, status) processing counts bucketed by second."""

    count: int
    created: AwareDatetime
    entrypoint: str
    priority: int
    status: JOB_STATUS


T = TypeVar("T")


@dataclasses.dataclass(frozen=True)
class ResourceKey(Generic[T]):
    """Typed handle for a shared resource stored in ``resources``.

    Define a key once and reuse it for type-checked lookups instead of bare
    string keys: ``name`` is the mapping key, ``type`` the expected runtime type.
    ``type`` must be a class usable with ``isinstance``.

    Usage example::

        DB = ResourceKey("db", asyncpg.Pool)
        pgq = PgQueuer(driver, resources={DB.name: pool})

        @pgq.entrypoint("work")
        async def work(job: Job, ctx: Context) -> None:
            pool = ctx.resource(DB)  # typed as asyncpg.Pool
    """

    name: str
    type: type[T]

    def resolve(self, resources: MutableMapping) -> T:
        """Look this key up in *resources*, checking presence and type."""
        try:
            value = resources[self.name]
        except KeyError:
            raise KeyError(f"resource {self.name!r} not registered in resources") from None
        if not isinstance(value, self.type):
            raise TypeError(
                f"resource {self.name!r} is {type(value).__name__}, expected {self.type.__name__}"
            )
        return value


@dataclasses.dataclass
class Context:
    """
    Runtime context shared across components.

    Attributes:
        cancellation: The root CancelScope controlling shutdown of running tasks.
        resources: A mutable mapping for user-provided, pre-initialized shared
            resources (e.g. DB pools, HTTP clients, ML models, caches). Always a
            mapping; never None. Users can mutate this at runtime if needed.
    """

    cancellation: anyio.CancelScope
    resources: MutableMapping = dataclasses.field(default_factory=dict)

    def resource(self, key: ResourceKey[T]) -> T:
        """Return the resource for *key*, typed as ``key.type``."""
        return key.resolve(self.resources)


@dataclasses.dataclass
class ScheduleContext:
    """
    Runtime context for scheduled tasks, mirroring Context for queue jobs.

    Attributes:
        resources: A mutable mapping for user-provided, pre-initialized shared
            resources (e.g. DB pools, HTTP clients, ML models, caches). Always a
            mapping; never None. Users can mutate this at runtime if needed.
    """

    resources: MutableMapping = dataclasses.field(default_factory=dict)

    def resource(self, key: ResourceKey[T]) -> T:
        """Return the resource for *key*, typed as ``key.type``."""
        return key.resolve(self.resources)


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
    status: JOB_STATUS
    entrypoint: CronEntrypoint


class TracebackRecord(BaseModel):
    job_id: JobId
    timestamp: datetime
    exception_type: str
    exception_message: str
    traceback: str
    additional_context: dict[str, Any] | None

    @classmethod
    def from_exception(
        cls,
        exc: Exception,
        job_id: JobId,
        additional_context: dict[str, Any] | None = None,
    ) -> TracebackRecord:
        return cls(
            job_id=job_id,
            timestamp=utc_now(),
            exception_type=exc.__class__.__name__,
            exception_message=str(exc),
            traceback="".join(traceback.format_exception(type(exc), exc, exc.__traceback__)),
            additional_context=additional_context,
        )
