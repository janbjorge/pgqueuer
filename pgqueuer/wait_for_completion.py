from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import chain

from . import db, models, qb, queries, tm

@dataclass
class WaitForCompletion:
    """
    Manages futures awaiting job completion events via PostgreSQL LISTEN/NOTIFY.

    Attributes:
        driver: Database driver supporting add_listener.
        waiters: Mapping from JobId to list of asyncio.Future awaiting completion.
        task_manager: TaskManager for scheduling background tasks.
    """
    driver: db.Driver
    waiters: defaultdict[models.JobId, list[asyncio.Future[models.JOB_STATUS]]] = field(
        default_factory=lambda: defaultdict(list),
        init=False,
    )
    task_manager: tm.TaskManager = field(
        default_factory=tm.TaskManager,
        init=False,
    )

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
        return f

    async def _on_change(self) -> None:
        """
        Handler triggered on database change notifications.
        Queries current status for all waiting jobs and resolves any terminal ones.
        """
        q = queries.Queries(self.driver)
        # Fetch statuses for all pending waiters
        results = await q.job_status(list(self.waiters.keys()))
        for jid, status in results:
            if self._is_terminal(status):
                for waiter in self.waiters.pop(jid, []):
                    if not waiter.done():
                        waiter.set_result(status)

    async def __aenter__(self) -> WaitForCompletion:
        """
        Enter async context: start listening for relevant change events.

        Returns:
            Self, for use in 'async with' constructs.
        """
        await self.driver.add_listener(qb.DBSettings().channel, self._is_relevant_event)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> bool:
        """
        Exit async context: wait for all pending waiters and gather tasks.

        Returns:
            False to propagate exceptions, if any.
        """
        # Await pending futures to ensure all results delivered
        await asyncio.gather(*chain.from_iterable(self.waiters.values()), return_exceptions=True)
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
