"""PostgreSQL NOTIFY event‑handling helpers
================================================

This module replaces the legacy *if/elif* `handle_event_type` chain with a
small, type‑safe dispatch system based on :class:`EventRouter`.  Public
components
-----------------

``EventRouter``
    Lightweight dispatcher that maps a single ``event.type`` string to a
    handler.  Handlers receive the concrete ``models.*Event`` subclass
    selected by *Pydantic*’s ``discriminator`` and **must** return
    ``None``.

``build_default_router``
    Convenience factory that wires handlers for the four canonical
    back‑end events (*table‑changed*, *requests‑per‑second*,
    *cancellation*, *health‑check*).  It injects application state via
    closures, avoiding a bulky context object.

``PGNoticeEventListener``
    A thin ``asyncio.Queue`` subclass used by the default
    *table‑changed* handler to deliver row‑level notifications to
    consumers.

``initialize_notice_event_listener``
    Glue that attaches any ``Callable[[models.AnyEvent], None]``
    (typically the router) to a live database connection through
    ``connection.add_listener``.

Typical usage::

    router = build_default_router(...state injections...)
    await initialize_notice_event_listener(conn, "mychannel", router)
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import MutableMapping, TypeAlias, TypeVar

from . import db, logconfig, models, types

EventHandler: TypeAlias = Callable[[models.Event], None]
HandlerTypeVar = TypeVar("HandlerTypeVar", bound=Callable[..., None])


class PGNoticeEventListener(asyncio.Queue[models.TableChangedEvent]):
    """Queue for PostgreSQL NOTIFY events."""


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
    statistics: MutableMapping[str, models.EntrypointStatistics],
    canceled: MutableMapping[models.JobId, models.Context],
    pending_health_check: MutableMapping[uuid.UUID, asyncio.Future[models.HealthCheckEvent]],
) -> EventRouter:
    """Return an `EventRouter` wired with handlers for all known event types."""

    router = EventRouter()

    @router.register("table_changed_event")
    def _table_changed(evt: models.TableChangedEvent) -> None:
        notice_event_queue.put_nowait(evt)

    @router.register("requests_per_second_event")
    def _requests_per_second(evt: models.RequestsPerSecondEvent) -> None:  #
        for entrypoint, count in evt.entrypoint_count.items():
            statistics[entrypoint].samples.append((count, evt.sent_at))

    @router.register("cancellation_event")
    def _cancellation(evt: models.CancellationEvent) -> None:
        for jid in evt.ids:
            if ctx := canceled.get(jid):
                ctx.cancellation.cancel()

    @router.register("health_check_event")
    def _health_check_event(evt: models.HealthCheckEvent) -> None:
        if fut := pending_health_check.get(evt.id):
            fut.set_result(evt)

    return router


async def initialize_notice_event_listener(
    connection: db.Driver,
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
