from __future__ import annotations

import asyncio

from . import models
from .db import Driver
from .logconfig import logger


class PGEventListener(asyncio.Queue[models.NoticeEvent]):
    """
    A PostgreSQL event queue that listens to a specified
    channel and stores incoming events.
    """


async def initialize_event_listener(
    connection: Driver,
    channel: models.PGChannel,
) -> PGEventListener:
    """
    This method establishes a listener on a PostgreSQL channel using
    the provided connection and channel.
    """

    def parse_and_queue(
        payload: str | bytes | bytearray,
        queue: PGEventListener,
    ) -> None:
        """
        Parses a JSON payload and inserts it into the queue as an `models.Event` object.
        """
        try:
            parsed = models.AnyEvnet.model_validate_json(payload)
        except Exception:
            logger.critical(
                "Failed to parse payload: `%s`.",
                payload,
            )
            return

        if parsed.type == "notice_event":
            try:
                queue.put_nowait(parsed)
            except Exception:
                logger.critical(
                    "Unexpected error inserting event into queue: `%s`.",
                    parsed,
                )

    listener = PGEventListener()
    await connection.add_listener(channel, lambda x: parse_and_queue(x, listener))
    return listener
