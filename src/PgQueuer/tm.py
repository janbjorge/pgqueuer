import asyncio
import dataclasses


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

    def add(self, task: asyncio.Task) -> None:
        """
        Adds an asyncio Task to the manager and registers a
        callback to automatically remove the task when it's done.
        """
        self.tasks.add(task)
        task.add_done_callback(self.tasks.remove)
