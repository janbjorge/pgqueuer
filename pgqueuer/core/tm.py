from __future__ import annotations

import asyncio
import dataclasses

from pgqueuer.core import logconfig


@dataclasses.dataclass
class TaskManager:
    """Tracks asyncio Tasks, logs unhandled exceptions, awaits them on __aexit__."""

    tasks: set[asyncio.Task] = dataclasses.field(
        default_factory=set,
        init=False,
    )

    def log_unhandled_exception(self, task: asyncio.Task) -> None:
        """Log non-cancellation exceptions raised by a finished task."""
        if not task.cancelled() and (exception := task.exception()):
            logconfig.logger.error(
                "Unhandled exception in task: %s",
                task,
                exc_info=exception,
            )

    def add(self, task: asyncio.Task) -> None:
        """Track *task*; auto-remove and log on completion."""
        self.tasks.add(task)
        task.add_done_callback(self.log_unhandled_exception)
        task.add_done_callback(self.tasks.remove)

    async def gather_tasks(self, return_exceptions: bool = True) -> list[BaseException | None]:
        """Await every tracked task and return per-task results/exceptions."""
        return await asyncio.gather(
            *self.tasks,
            return_exceptions=return_exceptions,
        )

    async def __aenter__(self) -> TaskManager:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.gather_tasks()
