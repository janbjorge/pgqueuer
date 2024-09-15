"""
Module for initializing and managing PostgreSQL event listeners.

This module defines the `PGNoticeEventListener` class, which extends `asyncio.Queue`
to store events received from a PostgreSQL NOTIFY channel. It also provides the
`initialize_notice_event_listener` function to set up the listener and handle
different types of events, such as table changes, request rates, and job cancellations.
"""

from __future__ import annotations

import asyncio

from . import db, logconfig, models


class PGNoticeEventListener(asyncio.Queue[models.TableChangedEvent]):
    """
    Asynchronous queue for PostgreSQL events from a specified channel.

    The `PGNoticeEventListener` class is a specialized `asyncio.Queue` that stores
    `TableChangedEvent` instances received from a PostgreSQL NOTIFY channel.
    It allows consumers to asynchronously retrieve events as they are received.
    """


async def initialize_notice_event_listener(
    connection: db.Driver,
    channel: models.PGChannel,
    statistics: dict[str, models.EntrypointStatistics],
    canceled: dict[models.JobId, models.Context],
) -> PGNoticeEventListener:
    """
    Initialize a listener on a PostgreSQL channel to handle various events.

    Sets up a listener on the specified PostgreSQL NOTIFY channel using the provided
    database connection. When notifications are received, it parses the payloads
    and handles different types of events, such as table changes, requests per second,
    and job cancellations.

    Args:
        connection (db.Driver): The database driver instance used to add the listener.
        channel (models.PGChannel): The name of the PostgreSQL channel to listen on.
        statistics (dict[str, models.EntrypointStatistics]): A dictionary to store
            request rate statistics for entrypoints.
        canceled (dict[models.JobId, models.Context]): A mapping of job IDs to their
            cancellation contexts.

    Returns:
        PGNoticeEventListener: An instance of `PGNoticeEventListener` that queues
            received `TableChangedEvent` instances.
    """

    def parse_and_queue(
        payload: str | bytes | bytearray,
        notice_event_queue: PGNoticeEventListener,
        statistics: dict[str, models.EntrypointStatistics],
    ) -> None:
        """
        Parse a notification payload and handle the event accordingly.

        Parses the JSON payload received from the PostgreSQL NOTIFY channel and processes
        it based on the event type. It supports handling table change events, requests
        per second events, and cancellation events.

        Args:
            payload (str | bytes | bytearray): The raw payload from the notification.
            notice_event_queue (PGNoticeEventListener): The event queue to put table
                change events into.
            statistics (dict[str, models.EntrypointStatistics]): A dictionary to update
                with requests per second statistics.
        """
        try:
            parsed = models.AnyEvent.model_validate_json(payload)
        except Exception as e:
            logconfig.logger.critical("Failed to parse payload: `%s`", payload, exc_info=e)
            return

        if parsed.root.type == "table_changed_event":
            notice_event_queue.put_nowait(parsed.root)
        elif parsed.root.type == "requests_per_second_event":
            statistics[parsed.root.entrypoint].samples.append(
                (parsed.root.count, parsed.root.sent_at)
            )
        elif parsed.root.type == "cancellation_event":
            for jid in parsed.root.ids:
                if ctx := canceled.get(jid):
                    ctx.cancellation.cancel()
        else:
            raise NotImplementedError(parsed, payload)

    notice_event_listener = PGNoticeEventListener()

    await connection.add_listener(
        channel,
        lambda x: parse_and_queue(x, notice_event_listener, statistics),
    )

    return notice_event_listener
