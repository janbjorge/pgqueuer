from __future__ import annotations

import asyncio

from . import db, logconfig, models


class PGNoticeEventListener(asyncio.Queue[models.TableChangedEvent]):
    """
    A PostgreSQL event queue that listens to a specified
    channel and stores incoming events.
    """


async def initialize_notice_event_listener(
    connection: db.Driver,
    channel: models.PGChannel,
    statistics: dict[str, models.EntrypointStatistics],
    canceled: dict[models.JobId, models.Context],
) -> PGNoticeEventListener:
    """
    Initializes a listener on a PostgreSQL channel, handling different types
    of events such as table changes, requests per second, and job cancellations.
    """

    def parse_and_queue(
        payload: str | bytes | bytearray,
        notice_event_queue: PGNoticeEventListener,
        statistics: dict[str, models.EntrypointStatistics],
    ) -> None:
        """
        Parses a JSON payload and inserts it into the queue as an `models.Event` object.
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
