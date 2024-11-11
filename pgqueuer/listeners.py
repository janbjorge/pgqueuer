"""
Module for initializing and managing PostgreSQL event listeners.

This module defines the `PGNoticeEventListener` class, which extends `asyncio.Queue`
to store events received from a PostgreSQL NOTIFY channel. It also provides the
`initialize_notice_event_listener` function to set up the listener and handle
different types of events, such as table changes, request rates, and job cancellations.
"""

from __future__ import annotations

import asyncio
from typing import Callable, MutableMapping

from . import db, logconfig, models


class PGNoticeEventListener(asyncio.Queue[models.TableChangedEvent]):
    """
    Queue for PostgreSQL NOTIFY events.
    """


def handle_event_type(
    event: models.AnyEvent,
    notice_event_queue: PGNoticeEventListener,
    statistics: MutableMapping[str, models.EntrypointStatistics],
    canceled: MutableMapping[models.JobId, models.Context],
) -> None:
    """
    Handle parsed event based on its type.
    """
    if event.root.type == "table_changed_event":
        notice_event_queue.put_nowait(event.root)
    elif event.root.type == "requests_per_second_event":
        for entrypoint, count in event.root.entrypoint_count.items():
            statistics[entrypoint].samples.append((count, event.root.sent_at))
    elif event.root.type == "cancellation_event":
        for jid in event.root.ids:
            if ctx := canceled.get(jid):
                ctx.cancellation.cancel()
    else:
        raise NotImplementedError(event)


async def initialize_notice_event_listener(
    connection: db.Driver,
    channel: models.PGChannel,
    event_handler: Callable[[models.AnyEvent], None],
) -> None:
    """
    Initialize listener on a PostgreSQL channel.
    """

    def process_notification_payload(payload: str | bytes | bytearray) -> None:
        """
        Process notification payload and handle event.

        Args:
            payload (str | bytes | bytearray): Raw notification payload.
        """
        try:
            parsed = models.AnyEvent.model_validate_json(payload)
        except Exception as e:
            logconfig.logger.critical(
                "Error while parsing notification payload: %s",
                payload,
                exc_info=e,
            )
            return

        try:
            event_handler(parsed)
        except Exception as e:
            logconfig.logger.critical(
                "Error while handling parsed event: %s",
                payload,
                exc_info=e,
            )
            return

    await connection.add_listener(channel, process_notification_payload)
