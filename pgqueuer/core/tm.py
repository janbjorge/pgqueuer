"""
Task Manager for handling asynchronous tasks.

This module provides the `TaskManager` class, which manages a collection of `asyncio.Task`
objects. It ensures proper tracking and cleanup of tasks, handles exceptions,
and supports context management for ease of use.
"""

from __future__ import annotations

import asyncio
import dataclasses

from . import logconfig


@dataclasses.dataclass
class TaskManager:
    """
    Manages a collection of asyncio Tasks, ensuring they are tracked and cleaned up appropriately.

    The `TaskManager` class allows you to add tasks to a managed set, automatically
    handles exceptions from tasks, and removes tasks from the set when they are done.
    It also provides asynchronous context management to facilitate clean startup and
    shutdown of tasks within an `async with` block.

    Attributes:
        tasks (set[asyncio.Task]): A set of asyncio Task objects being managed.
    """

    tasks: set[asyncio.Task] = dataclasses.field(
        default_factory=set,
        init=False,
    )

    def log_unhandled_exception(self, task: asyncio.Task) -> None:
        """
        Log any unhandled exceptions from a completed task.

        This method is a callback for tasks that have finished execution.
        If the task resulted in an exception, it logs the error using the application's logger.

        Args:
            task (asyncio.Task): The task that has completed.
        """

        # If the task has been cancelled; raises CancelledError.
        if not task.cancelled() and (exception := task.exception()):
            logconfig.logger.error(
                "Unhandled exception in task: %s",
                task,
                exc_info=exception,
            )

    def add(self, task: asyncio.Task) -> None:
        """
        Add an asyncio Task to the manager and set up callbacks.

        Registers the task with the manager's task set and adds callbacks to handle
        unhandled exceptions and to remove the task from the set when it is done.

        Args:
            task (asyncio.Task): The asyncio Task to be managed.
        """
        self.tasks.add(task)
        task.add_done_callback(self.log_unhandled_exception)
        task.add_done_callback(self.tasks.remove)

    async def gather_tasks(self, return_exceptions: bool = True) -> list[BaseException | None]:
        """
        Wait for all managed tasks to complete and gather their results.

        This method awaits all tasks currently in the manager's task set,
        collecting their results or exceptions. It ensures that all tasks
        have finished before proceeding.

        Returns:
            list[BaseException | None]: A list containing exceptions raised by tasks,
                or None for tasks that completed successfully.
        """
        return await asyncio.gather(
            *self.tasks,
            return_exceptions=return_exceptions,
        )

    async def __aenter__(self) -> TaskManager:
        """
        Enter the asynchronous context manager.

        Returns:
            TaskManager: The instance of the TaskManager itself.
        """
        return self

    async def __aexit__(self, *_: object) -> None:
        """
        Exit the asynchronous context manager, ensuring all tasks are gathered.

        Upon exiting the context, this method waits for all managed tasks to complete
        by calling `gather_tasks()`. This ensures that no tasks are left running when
        the context is exited.
        """
        await self.gather_tasks()
