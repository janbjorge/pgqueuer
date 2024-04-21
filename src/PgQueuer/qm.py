from __future__ import annotations

import asyncio
import dataclasses
from typing import Awaitable, Callable, TypeAlias, TypeVar

import asyncpg
from pgcachewatch.listeners import PGEventQueue
from pgcachewatch.models import PGChannel

from .logconfig import logger
from .models import Job
from .queries import PgQueuerLogQueries, PgQueuerQueries
from .tm import TaskManager

EntrypointFn: TypeAlias = Callable[[bytes | None], Awaitable[None]]
T = TypeVar("T", bound=EntrypointFn)


@dataclasses.dataclass
class QueueManager:
    pool: asyncpg.Pool
    q: PgQueuerQueries = dataclasses.field(init=False)
    ql: PgQueuerLogQueries = dataclasses.field(init=False)

    channel: PGChannel = dataclasses.field(
        default=PGChannel("ch_pgqueuer"),
        init=False,
    )
    alive: bool = dataclasses.field(
        init=False,
        default=True,
    )
    # Should registry be a weakref?
    registry: dict[str, EntrypointFn] = dataclasses.field(
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
        self.q = PgQueuerQueries(self.pool)
        self.ql = PgQueuerLogQueries(self.pool)

    def entrypoint(self, name: str) -> Callable[[T], T]:
        def register(func: T) -> T:
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
                while (jobs := await self.q.dequeue()).root:
                    for job in jobs.root:
                        self._dispatch(job)
                await listener.get()

            await asyncio.gather(*self.tm.tasks)

    def _dispatch(self, job: Job) -> None:
        async def runit() -> None:
            logger.debug(
                "Dispatching entrypoint/id: %s/%s",
                job.entrypoint,
                job.id,
            )
            try:
                await self.registry[job.entrypoint](job.payload)
            except Exception:
                logger.exception(
                    "Exception while processing entrypoint/id: %s/%s",
                    job.entrypoint,
                    job.id,
                )
                await self.ql.move_job_log(job, "exception")
            else:
                logger.debug(
                    "Dispatching entrypoint/id: %s/%s - successful",
                    job.entrypoint,
                    job.id,
                )
                await self.ql.move_job_log(job, "successful")

        self.tm.add(asyncio.create_task(runit()))
