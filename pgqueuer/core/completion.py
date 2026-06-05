from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import timedelta
from itertools import chain

from pgqueuer.core import tm
from pgqueuer.domain import models
from pgqueuer.domain.settings import DBSettings
from pgqueuer.ports.driver import Driver
from pgqueuer.ports.repository import QueueRepositoryPort


@dataclass
class CompletionWatcher:
    """Resolve per-job Futures when a job lands in a terminal state.

    Combines LISTEN/NOTIFY for instant updates with a periodic safety-net
    poll (``refresh_interval``) for missed notifications. NOTIFY bursts are
    coalesced into a single status query per ``debounce`` window.
    Terminal states: ``canceled``, ``deleted``, ``exception``, ``successful``.

    Usage example::

        async with CompletionWatcher(driver,
                                     refresh_interval=timedelta(seconds=2),
                                     debounce=timedelta(milliseconds=100)) as w:
            status = await w.wait_for(job_id)
            print("final status ->", status)
    """

    driver: Driver
    queries: QueueRepositoryPort = field(kw_only=True)
    refresh_interval: timedelta = field(
        default_factory=lambda: timedelta(seconds=5),
    )

    debounce: timedelta = field(
        default_factory=lambda: timedelta(milliseconds=50),
        repr=False,
    )
    waiters: defaultdict[models.JobId, list[asyncio.Future[models.JOB_STATUS]]] = field(
        default_factory=lambda: defaultdict(list),
        init=False,
        repr=False,
    )
    task_manager: tm.TaskManager = field(
        default_factory=tm.TaskManager,
        init=False,
        repr=False,
    )
    shutdown: asyncio.Event = field(
        default_factory=asyncio.Event,
        init=False,
        repr=False,
    )
    lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
        repr=False,
    )
    debounce_task: asyncio.Task[None] | None = field(
        default=None,
        init=False,
        repr=False,
    )

    async def __aenter__(self) -> "CompletionWatcher":
        self.task_manager.add(asyncio.create_task(self._poll_for_change()))
        await self.driver.add_listener(DBSettings().channel, self._is_relevant_event)
        self._schedule_refresh_waiters()
        return self

    async def __aexit__(self, *_: object) -> bool:
        self.shutdown.set()
        self._schedule_refresh_waiters()
        await asyncio.gather(*chain.from_iterable(self.waiters.values()))
        await self.task_manager.gather_tasks()
        return False

    def wait_for(self, jid: models.JobId) -> asyncio.Future[models.JOB_STATUS]:
        """Return a Future that resolves when *jid* reaches a terminal state."""
        fut: asyncio.Future[models.JOB_STATUS] = asyncio.get_running_loop().create_future()
        self.waiters[jid].append(fut)
        self._schedule_refresh_waiters()
        return fut

    def _schedule_refresh_waiters(self) -> None:
        """(Re)arm debounce timer; coalesces multiple triggers into one query."""
        if self.debounce_task and not self.debounce_task.done():
            return  # timer already running
        self.debounce_task = asyncio.create_task(self._debounced())
        self.task_manager.add(self.debounce_task)

    async def _debounced(self) -> None:
        """Sleep for the debounce interval, then run the consolidated check."""
        try:
            with suppress(TimeoutError, asyncio.TimeoutError):
                await asyncio.wait_for(
                    self.shutdown.wait(),
                    timeout=self.debounce.total_seconds(),
                )
        finally:
            self.debounce_task = None
        await self._refresh_waiters()

    def _is_relevant_event(self, payload: str | bytes | bytearray) -> None:
        """LISTEN/NOTIFY callback -- schedules a debounced change check."""
        try:
            evt = models.AnyEvent.model_validate_json(payload)
        except Exception:
            return
        if evt.root.type == "table_changed_event":
            self._schedule_refresh_waiters()

    async def _poll_for_change(self) -> None:
        """Safety-net poller for missed NOTIFY events."""
        while not self.shutdown.is_set():
            try:
                await asyncio.wait_for(
                    self.shutdown.wait(),
                    timeout=self.refresh_interval.total_seconds(),
                )
            except (TimeoutError, asyncio.TimeoutError):
                self._schedule_refresh_waiters()
            except asyncio.CancelledError:
                break

    async def _refresh_waiters(self) -> None:
        """Resolve Futures for waiting jobs that have reached a terminal state."""
        async with self.lock:
            for jid, status in await self.queries.job_status(list(self.waiters.keys())):
                if self._is_terminal(status):
                    for waiter in self.waiters.pop(jid, []):
                        if not waiter.done():
                            waiter.set_result(status)

    def _is_terminal(self, status: models.JOB_STATUS) -> bool:
        """Return ``True`` if *status* is terminal."""
        return status in ("canceled", "deleted", "exception", "successful")
