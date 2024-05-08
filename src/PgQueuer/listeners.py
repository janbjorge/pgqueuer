from __future__ import annotations

import asyncio

import asyncpg

from . import models
from .logconfig import logger


def _critical_termination_listener(*_: object, **__: object) -> None:
    # Must be defined in the global namespace, as ayncpg keeps
    # a set of functions to call. This this will now happen once as
    # all instance will point to the same function.
    logger.critical("Connection is closed / terminated.")


class PGEventListener(asyncio.Queue[models.Event]):
    """
    A PostgreSQL event queue that listens to a specified
    channel and stores incoming events.
    """


async def initialize_event_listener(
    pg_connection: asyncpg.Connection,
    pg_channel: models.PGChannel,
) -> PGEventListener:
    """
    This method establishes a listener on a PostgreSQL channel using
    the provided connection and channel.
    """

    def parse_and_queue(
        payload: str | bytes | bytearray, queue: PGEventListener
    ) -> None:
        """
        Parses a JSON payload and inserts it into the queue as an `models.Event` object.
        """
        try:
            parsed_event = models.Event.model_validate_json(payload)
        except Exception:
            logger.critical(
                "Failed to parse payload: `%s`.",
                payload,
            )
            return

        try:
            queue.put_nowait(parsed_event)
        except Exception:
            logger.critical(
                "Unexpected error inserting event into queue: `%s`.",
                parsed_event,
            )

    listener = PGEventListener()
    await pg_connection.add_listener(
        pg_channel, lambda *x: parse_and_queue(x[-1], listener)
    )
    pg_connection.add_termination_listener(_critical_termination_listener)
    return listener
