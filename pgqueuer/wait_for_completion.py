from __future__ import annotations

import asyncio
import contextlib
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import timedelta
from itertools import chain

from . import db, models, qb, queries, tm


@dataclass
class CompletionWatcher:
    """
    Asynchronously waits for PostgreSQL-backed jobs to reach a terminal state.

    The waiter subscribes to LISTEN/NOTIFY so that it can react immediately to
    `table_changed_event` notifications emitted by the job table.  In addition,
    it performs a *periodic refresh* every `refresh_interval` seconds:

        *Purpose* – the extra query acts as a safety-net against lost NOTIFY
        messages (e.g. on network hiccups, connection pool swaps, or missed
        events that occurred before the listener was added).

        *Behaviour* – if `refresh_interval` is:
            • a positive ``timedelta`` (default **5 s**): a background task
              re-checks outstanding job IDs at that cadence;
            • ``None``: periodic polling is disabled and only NOTIFY events
              are used.

    Example
    -------
    >>> async with JobCompletionWaiter(driver, refresh_interval=timedelta(seconds=2)) as w:
    ...     status = await w.wait_for(job_id)
    ...     print(status)

    Terminal states currently recognised: ``canceled``, ``deleted``,
    ``exception``, ``successful``.
    """

    driver: db.Driver
    refresh_interval: timedelta | None = timedelta(seconds=5)

    q: queries.Queries = field(
        init=False,
    )
    waiters: defaultdict[models.JobId, list[asyncio.Future[models.JOB_STATUS]]] = field(
        default_factory=lambda: defaultdict(list),
        init=False,
    )
    task_manager: tm.TaskManager = field(
        default_factory=tm.TaskManager,
        init=False,
    )
    shutdown: asyncio.Event = field(
        default_factory=asyncio.Event,
        init=False,
    )
    lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
    )

    def __post_init__(self) -> None:
        self.q = queries.Queries(self.driver)

    def wait_for(self, jid: models.JobId) -> asyncio.Future[models.JOB_STATUS]:
        """
        Create and register a Future that will be resolved when the job reaches a terminal state.

        Args:
            jid: The JobId to wait on.

        Returns:
            An asyncio.Future that will yield the job's final status.
        """
        f: asyncio.Future[models.JOB_STATUS] = asyncio.get_running_loop().create_future()
        self.waiters[jid].append(f)
        f.add_done_callback(lambda x: self.waiters.get(jid, []).remove(x))
        self.task_manager.add(asyncio.create_task(self._on_change()))
        return f

    async def _on_change(self) -> None:
        """
        Handler triggered on database change notifications.
        Queries current status for all waiting jobs and resolves any terminal ones.
        """
        async with self.lock:
            for jid, status in await self.q.job_status(list(self.waiters.keys())):
                if self._is_terminal(status):
                    for waiter in self.waiters.pop(jid, []):
                        waiter.set_result(status)

    async def _poll_for_change(self) -> None:
        refresh_interval = self.refresh_interval
        if refresh_interval is None:
            return
        while not self.shutdown.is_set():
            with contextlib.suppress(TimeoutError, asyncio.TimeoutError):
                await asyncio.wait_for(
                    self.shutdown.wait(),
                    timeout=refresh_interval.total_seconds(),
                )

    async def __aenter__(self) -> CompletionWatcher:
        """
        Enter async context: start listening for relevant change events.

        Returns:
            Self, for use in 'async with' constructs.
        """
        # Check if any tasks are already completed before starting the listener
        self.task_manager.add(asyncio.create_task(self._poll_for_change()))
        await self.driver.add_listener(qb.DBSettings().channel, self._is_relevant_event)
        return self

    async def __aexit__(self, *_: object) -> bool:
        """
        Exit async context: wait for all pending waiters and gather tasks.

        Returns:
            False to propagate exceptions, if any.
        """
        self.shutdown.set()
        self.task_manager.add(asyncio.create_task(self._on_change()))
        # Await pending futures to ensure all results delivered
        await asyncio.gather(*chain.from_iterable(self.waiters.values()))
        # Ensure all background tasks are complete
        await self.task_manager.gather_tasks()
        return False

    def _is_terminal(self, status: models.JOB_STATUS) -> bool:
        """
        Check if a given status is one of the terminal states.

        Args:
            status: The JOB_STATUS string to evaluate.

        Returns:
            True if status is terminal, False otherwise.
        """
        return status in ("canceled", "deleted", "exception", "successful")

    def _is_relevant_event(
        self,
        payload: str | bytes | bytearray,
    ) -> None:
        """
        Callback for LISTEN notifications. Schedules handling if event is relevant.

        Args:
            payload: Raw JSON payload from NOTIFY.
        """
        try:
            evt = models.AnyEvent.model_validate_json(payload)
        except Exception:
            return
        if evt.root.type == "table_changed_event":
            self.task_manager.add(asyncio.create_task(self._on_change()))
