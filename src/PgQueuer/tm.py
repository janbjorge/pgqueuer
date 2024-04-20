import asyncio
import dataclasses


@dataclasses.dataclass
class TaskManager:
    tasks: set[asyncio.Task] = dataclasses.field(
        default_factory=set,
        init=False,
    )

    def add(self, task: asyncio.Task) -> None:
        self.tasks.add(task)
        task.add_done_callback(self.tasks.remove)
