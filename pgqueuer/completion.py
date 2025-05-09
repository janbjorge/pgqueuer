from __future__ import annotations

import asyncio
from collections import defaultdict
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import timedelta
from itertools import chain

from . import db, models, qb, queries, tm


@dataclass
class CompletionWatcher:
    """
    Watches a set of PostgreSQL-backed jobs and resolves user Futures as soon as
    each job lands in a *terminal* state.

    ----------
    How it works
    ----------
    1. **Instant updates via LISTEN/NOTIFY**

       The watcher listens on the ``table_changed_event`` channel emitted by the
       job table.  Whenever a notification arrives it *schedules* (but does not
       immediately run) a status query.

    2. **Safety-net polling (`refresh_interval`)**

       Lost NOTIFY messages can happen – network blips, a pooled connection
       being swapped out, or events that occurred *before* the listener was
       attached.  To cover these gaps a lightweight polling task runs every

       ``refresh_interval`` (default **5 s**).  Set this parameter to
       ``None`` to disable polling entirely if LISTEN reliability is good
       enough for your deployment.

    3. **Event coalescing (`debounce`)**

       Bursts of NOTIFYs (for example when 100 rows are updated in one
       transaction) could otherwise trigger 100 identical queries.  Instead, a
       debouncing timer is used:

       * On the **first** trigger a timer is (re)armed for the length of
         ``debounce`` (default **50 ms**).
       * Any further triggers that arrive while the timer is running are *ignored*.
       * When the timer fires a single consolidated query is executed.

       This keeps load predictable without delaying feedback beyond the small
       debounce window.

    ----------
    Parameters
    ----------
    driver : `db.Driver`
        An async DB driver that can both execute queries and register LISTEN
        callbacks.
    refresh_interval : `datetime.timedelta | None`
        Cadence of the periodic safety-net query.  ``None`` disables polling.
    debounce : `datetime.timedelta`
        Length of the coalescing window that limits how often the expensive
        status query is run.

    ----------
    Usage example
    ----------
    ```python
    async with CompletionWatcher(driver,
                                 refresh_interval=timedelta(seconds=2),
                                 debounce=timedelta(milliseconds=100)) as w:
        status = await w.wait_for(job_id)
        print("final status →", status)
    ```

    ----------
    Terminal states recognised
    ----------
    ``canceled``, ``deleted``, ``exception``, ``successful``.
    """

    driver: db.Driver
    refresh_interval: timedelta = field(
        default_factory=lambda: timedelta(seconds=5),
    )

    """Time-window in which multiple change triggers are coalesced into one."""
    debounce: timedelta = field(
        default_factory=lambda: timedelta(milliseconds=50),
        repr=False,
    )

    # ─────────────────────────── internals ────────────────────────────
    q: queries.Queries = field(
        init=False,
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

    # ───────────────────────── life-cycle ────────────────────────────
    def __post_init__(self) -> None:
        """Instantiate query helper once the dataclass has been created."""
        self.q = queries.Queries(self.driver)

    async def __aenter__(self) -> "CompletionWatcher":
        """
        Enter async context:

        * start periodic polling (if enabled);
        * register LISTEN/NOTIFY callback;
        * schedule an immediate status probe.
        """
        self.task_manager.add(asyncio.create_task(self._poll_for_change()))
        await self.driver.add_listener(qb.DBSettings().channel, self._is_relevant_event)
        self._schedule_refresh_waiters()
        return self

    async def __aexit__(self, *_: object) -> bool:
        """
        Exit async context:

        * stop background tasks;
        * flush a final change check;
        * wait for all user Futures to resolve.
        """
        self.shutdown.set()
        self._schedule_refresh_waiters()
        await asyncio.gather(*chain.from_iterable(self.waiters.values()))
        await self.task_manager.gather_tasks()
        return False

    # ───────────────────────── public API ────────────────────────────
    def wait_for(self, jid: models.JobId) -> asyncio.Future[models.JOB_STATUS]:
        """
        Return a Future that resolves when *jid* reaches a terminal state.

        Args:
            jid: ``JobId`` to watch.

        Returns:
            An ``asyncio.Future`` yielding the final ``JOB_STATUS``.
        """
        fut: asyncio.Future[models.JOB_STATUS] = asyncio.get_running_loop().create_future()
        self.waiters[jid].append(fut)
        self._schedule_refresh_waiters()
        return fut

    # ────────────────── debounce / scheduling helpers ────────────────
    def _schedule_refresh_waiters(self) -> None:
        """
        (Re)arm the debounce timer so that `_on_change` is executed at most once
        in every ``debounce`` window irrespective of how many triggers arrive.
        """
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

    # ────────────────── event handlers & polling ─────────────────────
    def _is_relevant_event(self, payload: str | bytes | bytearray) -> None:
        """LISTEN/NOTIFY callback – schedules a debounced change check."""
        try:
            evt = models.AnyEvent.model_validate_json(payload)
        except Exception:
            return
        if evt.root.type == "table_changed_event":
            self._schedule_refresh_waiters()

    async def _poll_for_change(self) -> None:
        """
        Periodic safety-net that re-checks outstanding jobs every
        ``refresh_interval``.  Disabled when ``refresh_interval`` is ``None``.
        """
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
        """
        Query the current status for all waiting jobs and resolve Futures whose
        job has reached a terminal state.
        """
        async with self.lock:
            for jid, status in await self.q.job_status(list(self.waiters.keys())):
                if self._is_terminal(status):
                    for waiter in self.waiters.pop(jid, []):
                        waiter.set_result(status)

    # ─────────────────────── helper methods ─────────────────────────
    def _is_terminal(self, status: models.JOB_STATUS) -> bool:
        """Return ``True`` if *status* is terminal."""
        return status in ("canceled", "deleted", "exception", "successful")
