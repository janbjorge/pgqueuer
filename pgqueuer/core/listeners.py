"""PostgreSQL NOTIFY event-routing helpers."""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import timedelta
from typing import MutableMapping, TypeAlias, TypeVar

from typing_extensions import assert_never

from pgqueuer.core import logconfig
from pgqueuer.domain import models, types
from pgqueuer.ports.driver import Driver

EventHandler: TypeAlias = Callable[[models.Event], None]
HandlerTypeVar = TypeVar("HandlerTypeVar", bound=Callable[..., None])


class PGNoticeEventListener(asyncio.Queue[models.TableChangedEvent]):
    """Queue for PostgreSQL NOTIFY events."""

    def drain_nowait(self) -> None:
        """Discard all buffered events; a completed dequeue cycle already consumed the work."""
        with contextlib.suppress(asyncio.QueueEmpty):
            while True:
                self.get_nowait()


@dataclass
class EventRouter:
    """Dispatch parsed NOTIFY events to exactly one handler per `type`."""

    handlers: dict[types.EVENT_TYPES, EventHandler] = field(default_factory=dict, init=False)

    def register(self, event_type: types.EVENT_TYPES) -> Callable[[HandlerTypeVar], HandlerTypeVar]:
        """Decorator that wires *func* to *event_type*. The function must return `None`."""

        def decorator(func: HandlerTypeVar) -> HandlerTypeVar:
            if event_type in self.handlers:
                raise ValueError(f"duplicate handler for {event_type!r}")
            self.handlers[event_type] = func
            return func

        return decorator

    def __call__(self, envelope: models.AnyEvent) -> None:
        event = envelope.root
        try:
            self.handlers[event.type](event)
        except KeyError as exc:
            raise NotImplementedError(event) from exc


def default_event_router(
    *,
    notice_event_queue: PGNoticeEventListener,
    canceled: MutableMapping[models.JobId, models.Context],
    pending_health_check: MutableMapping[uuid.UUID, asyncio.Future[models.HealthCheckEvent]],
) -> EventRouter:
    """Return an `EventRouter` wired with handlers for all known event types."""

    router = EventRouter()

    @router.register("table_changed_event")
    def _table_changed(evt: models.TableChangedEvent) -> None:
        if evt.operation == "insert" or evt.operation == "update":
            notice_event_queue.put_nowait(evt)
        elif evt.operation == "delete" or evt.operation == "truncate":
            # Removed rows can never make a job available; waking the
            # dequeue loop for these would be a pointless herd wake-up.
            return
        else:
            assert_never(evt.operation)

    @router.register("cancellation_event")
    def _cancellation(evt: models.CancellationEvent) -> None:
        for jid in evt.ids:
            if ctx := canceled.get(jid):
                ctx.cancellation.cancel()

    @router.register("health_check_event")
    def _health_check_event(evt: models.HealthCheckEvent) -> None:
        if (fut := pending_health_check.get(evt.id)) and not fut.done():
            fut.set_result(evt)

    return router


async def initialize_notice_event_listener(
    connection: Driver,
    channel: models.Channel,
    event_handler: Callable[[models.AnyEvent], None],
) -> None:
    """Add a listener on *channel* and funnel parsed events to *event_handler*."""

    def _process_payload(payload: str | bytes | bytearray) -> None:
        try:
            parsed = models.AnyEvent.model_validate_json(payload)
        except Exception as exc:
            logconfig.logger.critical(
                "Error parsing notification payload: %s", payload, exc_info=exc
            )
            return

        try:
            event_handler(parsed)
        except Exception as exc:
            logconfig.logger.critical(
                "Error while handling parsed event: %s", payload, exc_info=exc
            )

    await connection.add_listener(channel, _process_payload)


def wait_for_notice_event(
    queue: PGNoticeEventListener,
    timeout: timedelta,
) -> asyncio.Task[models.TableChangedEvent | None]:
    """Wait for a table change event with a timeout, returning None on expiry."""

    async def suppressed_timeout() -> models.TableChangedEvent | None:
        with contextlib.suppress(asyncio.TimeoutError):
            return await asyncio.wait_for(
                queue.get(),
                timeout=timeout.total_seconds(),
            )
        return None

    return asyncio.create_task(suppressed_timeout())
