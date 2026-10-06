from __future__ import annotations

import asyncio
import contextlib
import dataclasses
from typing import AsyncGenerator, TypeVar

from pgqueuer.core import logconfig

T = TypeVar("T")


def discard_outcome(task: asyncio.Task[T]) -> None:
    """Mark *task*'s exception as retrieved so asyncio does not log it."""
    if not task.cancelled():
        task.exception()


async def cancel_and_wait(task: asyncio.Task[T]) -> None:
    """Cancel *task*, wait until it is done, and discard its outcome.

    ``asyncio.wait`` does not forward a cancel of the caller to *task*, so the
    caller's own cancellation still propagates while *task* unwinds.
    """
    task.add_done_callback(discard_outcome)
    task.cancel()
    await asyncio.wait({task})


@contextlib.asynccontextmanager
async def cancel_on_exit(task: asyncio.Task[T]) -> AsyncGenerator[asyncio.Task[T], None]:
    """Yield *task*; on exit cancel it and discard its outcome.

    The task's result or exception is intentionally swallowed: callers that
    care about the outcome must observe it before the context exits.
    """
    try:
        yield task
    finally:
        await cancel_and_wait(task)


@dataclasses.dataclass
class TaskManager:
    """Tracks asyncio Tasks, logs unhandled exceptions, awaits them on __aexit__."""

    tasks: set[asyncio.Task[object]] = dataclasses.field(
        default_factory=set,
        init=False,
    )

    def log_unhandled_exception(self, task: asyncio.Task[object]) -> None:
        """Log non-cancellation exceptions raised by a finished task."""
        if not task.cancelled() and (exception := task.exception()):
            logconfig.logger.error(
                "Unhandled exception in task: %s",
                task,
                exc_info=exception,
            )

    def add(self, task: asyncio.Task[object]) -> None:
        """Track *task*; auto-remove and log on completion."""
        self.tasks.add(task)
        task.add_done_callback(self.log_unhandled_exception)
        task.add_done_callback(self.tasks.discard)

    async def gather_tasks(self, return_exceptions: bool = True) -> list[object]:
        """Await every tracked task and return per-task results/exceptions."""
        results: list[object] = await asyncio.gather(
            *self.tasks,
            return_exceptions=return_exceptions,
        )
        return results

    async def __aenter__(self) -> TaskManager:
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.gather_tasks()
