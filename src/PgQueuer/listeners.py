from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from datetime import datetime

from . import models
from .db import Driver
from .logconfig import logger


class PGNoticeEventListener(asyncio.Queue[models.TableChangedEvent]):
    """
    A PostgreSQL event queue that listens to a specified
    channel and stores incoming events.
    """


async def initialize_notice_event_listener(
    connection: Driver,
    channel: models.PGChannel,
    statistics: defaultdict[str, deque[tuple[int, datetime]]],
) -> PGNoticeEventListener:
    """
    This method establishes a listener on a PostgreSQL channel using
    the provided connection and channel.
    """

    def parse_and_queue(
        payload: str | bytes | bytearray,
        notice_event_queue: PGNoticeEventListener,
        statistics: defaultdict[str, deque[tuple[int, datetime]]],
    ) -> None:
        """
        Parses a JSON payload and inserts it into the queue as an `models.Event` object.
        """
        try:
            parsed = models.AnyEvent.model_validate_json(payload)
        except Exception as e:
            logger.critical("Failed to parse payload: `%s`, `%s`", payload, e)
            return

        if parsed.root.type == "table_changed_event":
            notice_event_queue.put_nowait(parsed.root)
        elif parsed.root.type == "requests_per_second_event":
            statistics[parsed.root.entrypoint].append((parsed.root.count, parsed.root.sent_at))
        else:
            raise NotImplementedError(parsed, payload)

    notice_event_listener = PGNoticeEventListener()

    await connection.add_listener(
        channel,
        lambda x: parse_and_queue(x, notice_event_listener, statistics),
    )

    return notice_event_listener
