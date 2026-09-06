"""PostgreSQL NOTIFY event-routing helpers."""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import MutableMapping

from typing_extensions import assert_never

from pgqueuer.core import logconfig
from pgqueuer.domain import models, types
from pgqueuer.ports.driver import Driver


class PGNoticeEventListener(asyncio.Queue[models.TableChangedEvent]):
    """Queue for PostgreSQL NOTIFY events."""


@dataclass
class EventRouter:
    """Dispatch a parsed NOTIFY envelope to the handler for its event type."""

    on_table_changed: Callable[[models.TableChangedEvent], None]
    on_cancellation: Callable[[models.CancellationEvent], None]
    on_health_check: Callable[[models.HealthCheckEvent], None]

    def __call__(self, envelope: models.AnyEvent) -> None:
        event = envelope.root
        if isinstance(event, models.TableChangedEvent):
            self.on_table_changed(event)
        elif isinstance(event, models.CancellationEvent):
            self.on_cancellation(event)
        elif isinstance(event, models.HealthCheckEvent):
            self.on_health_check(event)
        else:
            assert_never(event)


def default_event_router(
    *,
    notice_event_queue: PGNoticeEventListener,
    canceled: MutableMapping[types.JobId, models.Context],
    pending_health_check: MutableMapping[
        types.HealthCheckId, asyncio.Future[models.HealthCheckEvent]
    ],
) -> EventRouter:
    """Return an `EventRouter` wired with handlers for all known event types."""

    def on_table_changed(evt: models.TableChangedEvent) -> None:
        notice_event_queue.put_nowait(evt)

    def on_cancellation(evt: models.CancellationEvent) -> None:
        for jid in evt.ids:
            if ctx := canceled.get(jid):
                ctx.cancellation.cancel()

    def on_health_check(evt: models.HealthCheckEvent) -> None:
        if (fut := pending_health_check.get(evt.id)) and not fut.done():
            fut.set_result(evt)

    return EventRouter(
        on_table_changed=on_table_changed,
        on_cancellation=on_cancellation,
        on_health_check=on_health_check,
    )


async def initialize_notice_event_listener(
    connection: Driver,
    channel: types.Channel,
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
