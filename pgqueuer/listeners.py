from __future__ import annotations

import asyncio
from collections import deque
from datetime import datetime

from . import models
from .db import Driver
from .logconfig import logger
from .models import AnyEvent, Context, PGChannel, TableChangedEvent


class PGNoticeEventListener(asyncio.Queue[TableChangedEvent]):
    """
    A PostgreSQL event queue that listens to a specified
    channel and stores incoming events.
    """


async def initialize_notice_event_listener(
    connection: Driver,
    channel: PGChannel,
    statistics: dict[str, deque[tuple[int, datetime]]],
    canceled: dict[models.JobId, Context],
) -> PGNoticeEventListener:
    """
    Initializes a listener on a PostgreSQL channel, handling different types
    of events such as table changes, requests per second, and job cancellations.
    """

    def parse_and_queue(
        payload: str | bytes | bytearray,
        notice_event_queue: PGNoticeEventListener,
        statistics: dict[str, deque[tuple[int, datetime]]],
    ) -> None:
        """
        Parses a JSON payload and inserts it into the queue as an `models.Event` object.
        """
        try:
            parsed = AnyEvent.model_validate_json(payload)
        except Exception as e:
            logger.critical("Failed to parse payload: `%s`", payload, exc_info=e)
            return

        if parsed.root.type == "table_changed_event":
            notice_event_queue.put_nowait(parsed.root)
        elif parsed.root.type == "requests_per_second_event":
            statistics[parsed.root.entrypoint].append((parsed.root.count, parsed.root.sent_at))
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
