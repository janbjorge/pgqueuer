from __future__ import annotations

import asyncio
import dataclasses
from typing import Awaitable, Callable, TypeVar

import asyncpg
from pgcachewatch.listeners import PGEventQueue
from pgcachewatch.models import PGChannel

from .logconfig import logger
from .models import Job
from .queries import Queries

C = TypeVar("C", bound=Callable[[bytes], Awaitable[None]])


@dataclasses.dataclass
class TaskManager:
    tasks: set[asyncio.Task] = dataclasses.field(default_factory=set)

    def add(self, task: asyncio.Task) -> None:
        self.tasks.add(task)
        task.add_done_callback(self.tasks.remove)


@dataclasses.dataclass
class QueueManager:
    pool: asyncpg.Pool
    q: Queries

    channel: PGChannel = dataclasses.field(
        default=PGChannel("ch_pgqueuer"),
    )
    alive: bool = dataclasses.field(
        init=False,
        default=True,
    )
    registry: dict[str, Callable[[bytes], Awaitable[None]]] = dataclasses.field(
        init=False,
        default_factory=dict,
    )
    tm: TaskManager = dataclasses.field(
        init=False,
        default_factory=TaskManager,
    )

    def __post_init__(self) -> None:
        if self.pool.get_min_size() < 1:
            raise ValueError("... min size must be gt 1.")

    def entrypoint(self, name: str) -> Callable[[C], C]:
        def register(func: C) -> C:
            if name in self.registry:
                raise RuntimeError(f"{name} already in registry, must be unique.")
            self.registry[name] = func
            return func

        return register

    async def run(self) -> None:
        async with self.pool.acquire() as conn:
            listener = PGEventQueue()
            await listener.connect(conn, self.channel)

            while self.alive:
                while (jobs := await self.q.next_jobs()).root:
                    for job in jobs.root:
                        logger.debug("Dispatching %s(...)", job.entrypoint)
                        self._dispatch(job)
                await listener.get()

            await asyncio.gather(*self.tm.tasks)

    def _dispatch(self, job: Job) -> None:
        async def runit() -> None:
            try:
                await self.registry[job.entrypoint](job.payload)
            except Exception:
                logger.exception(
                    "Exception while processing entrypoint: %s", job.entrypoint
                )
                await self.q.move_job_log(job, "exception")
            else:
                await self.q.move_job_log(job, "successful")

        self.tm.add(asyncio.create_task(runit()))
