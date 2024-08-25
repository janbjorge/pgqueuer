from __future__ import annotations

import asyncio
import dataclasses

from .logconfig import logger


@dataclasses.dataclass
class TaskManager:
    """
    Manages a collection of asyncio Tasks, keeping track of active
    tasks and removing them once they are complete.
    """

    tasks: set[asyncio.Task] = dataclasses.field(
        default_factory=set,
        init=False,
    )

    def log_unhandled_exception(self, task: asyncio.Task) -> None:
        if exception := task.exception():
            logger.error("Unhandled exception in: %s", task, exc_info=exception)

    def add(self, task: asyncio.Task) -> None:
        """
        Adds an asyncio Task to the manager and registers a
        callback to automatically remove the task when it's done.
        """
        self.tasks.add(task)
        task.add_done_callback(self.log_unhandled_exception)
        task.add_done_callback(self.tasks.remove)

    async def __aenter__(self) -> TaskManager:
        return self

    async def __aexit__(self, *_: object) -> None:
        await asyncio.gather(*self.tasks, return_exceptions=True)
