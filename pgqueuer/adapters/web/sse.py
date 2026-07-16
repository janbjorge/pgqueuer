from __future__ import annotations

import asyncio
import contextlib
import dataclasses
from collections.abc import AsyncGenerator
from datetime import timedelta

from pgqueuer.core.listeners import initialize_notice_event_listener
from pgqueuer.domain import models
from pgqueuer.ports.driver import Driver

KEEPALIVE_SECONDS = 15.0
SUBSCRIBER_QUEUE_SIZE = 8


@dataclasses.dataclass
class Broadcaster:
    """Fan NOTIFY-driven change signals out to SSE subscribers, debounced.

    Listens on the existing PgQueuer channel; every queue-table change (or
    cancellation event) schedules a single debounced ``queue-change`` push to
    all connected subscribers, so NOTIFY storms collapse into one refresh.
    """

    driver: Driver
    channel: models.Channel
    debounce: timedelta = dataclasses.field(default=timedelta(milliseconds=250))

    subscribers: set[asyncio.Queue[str]] = dataclasses.field(default_factory=set, init=False)
    flush_task: asyncio.Task[None] | None = dataclasses.field(default=None, init=False)

    async def start(self) -> None:
        await initialize_notice_event_listener(self.driver, self.channel, self.route_event)

    def route_event(self, envelope: models.AnyEvent) -> None:
        if isinstance(envelope.root, (models.TableChangedEvent, models.CancellationEvent)) and (
            self.flush_task is None or self.flush_task.done()
        ):
            self.flush_task = asyncio.create_task(self.flush_after_debounce())

    async def flush_after_debounce(self) -> None:
        await asyncio.sleep(self.debounce.total_seconds())
        for queue in self.subscribers:
            with contextlib.suppress(asyncio.QueueFull):
                queue.put_nowait("queue-change")

    async def stream(self) -> AsyncGenerator[str, None]:
        """Yield SSE frames for one subscriber; keepalive comments bridge quiet spells."""
        queue = asyncio.Queue[str](maxsize=SUBSCRIBER_QUEUE_SIZE)
        self.subscribers.add(queue)
        try:
            yield "retry: 3000\n\n"
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=KEEPALIVE_SECONDS)
                    yield f"event: {event}\ndata: {{}}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            self.subscribers.discard(queue)
