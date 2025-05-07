from __future__ import annotations

import asyncio
from collections import defaultdict
from dataclasses import dataclass, field
from itertools import chain

from . import db, models, qb, queries, tm


@dataclass
class WaitForCompletion:
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
        f = asyncio.Future[models.JOB_STATUS]()
        self.waiters[jid].append(f)
        return f

    async def _on_change(self) -> None:
        q = queries.Queries(self.driver)
        for jid, status in await q.job_status(list(self.waiters.keys())):
            if self._is_terminal(status or "successful"):
                for waiter in self.waiters.get(jid, []):
                    waiter.set_result(status or "successful")

                # Drop interal ref. no need to ever re-update.
                self.waiters.pop(jid, None)

    async def __aenter__(self):
        await self.driver.add_listener(qb.DBSettings().channel, self._is_relevant_event)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await asyncio.gather(*chain.from_iterable(self.waiters.values()))
        await self.task_manager.gather_tasks()
        return False

    def _is_terminal(self, status: models.JOB_STATUS) -> bool:
        """
        Determine if a status is in a terminal state.
        """
        return status in (
            "canceled",
            "deleted",
            "exception",
            "successful",
        )

    def _is_relevant_event(
        self,
        payload: str | bytes | bytearray,
    ) -> None:
        """
        Parse a JSON notify payload and check if it is a table_changed_event
        for the given table.
        """
        try:
            evt = models.AnyEvent.model_validate_json(payload)
        except Exception:
            return
        if evt.root.type == "table_changed_event":
            self.task_manager.add(asyncio.create_task(self._on_change()))
