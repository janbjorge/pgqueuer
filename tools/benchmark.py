import argparse
import asyncio
import contextlib
import random
import time
from datetime import datetime, timedelta
from typing import Callable, Generator

import asyncpg
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager
from PgQueuer.queries import Queries


@contextlib.contextmanager
def execution_timer() -> (
    Generator[
        Callable[[], timedelta],
        None,
        None,
    ]
):
    enter = time.perf_counter()
    done: float | None = None
    try:
        yield lambda: timedelta(seconds=((done or time.perf_counter()) - enter))
    finally:
        done = time.perf_counter()


async def run_qm(pool: asyncpg.Pool) -> None:
    qm = QueueManager(pool)

    @qm.entrypoint("async")
    async def asyncfetch(job: Job) -> None:
        if job.payload is None:
            qm.alive = False

    @qm.entrypoint("sync")
    def syncfetch(job: Job) -> None:
        if job.payload is None:
            qm.alive = False

    await qm.run(timedelta(seconds=0))


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the job processing benchmark.",
    )
    parser.add_argument(
        "-t",
        "--time-limit",
        type=lambda x: timedelta(seconds=float(x)),
        default=timedelta(seconds=10),
        help="Run the benchmark for a limited number of seconds. Default is 10",
    )
    args = parser.parse_args()
    start = datetime.now()

    async with asyncpg.create_pool() as pool:
        queries = Queries(pool)
        await queries.clear_log()
        while True:
            for concurrecy in range(1, 6):
                N = concurrecy * 1_000

                await queries.clear_queue()

                entrypoints = ["sync"] * (N // 2) + ["async"] * (N // 2)
                random.shuffle(entrypoints)
                payloads = [f"{n}".encode() for n in range(N)]
                priorities = [0] * N
                await queries.enqueue(entrypoints, payloads, priorities)
                await queries.enqueue(
                    ["async"] * concurrecy**2,
                    [None] * concurrecy**2,
                    [0] * concurrecy**2,
                )

                with execution_timer() as elapsed:
                    await asyncio.gather(
                        *[run_qm(pool) for _ in range(concurrecy)],
                    )

                elapsed_total_seconds = elapsed().total_seconds()
                jps = (N - len(await queries.queue_size())) / elapsed_total_seconds

                print(
                    f"Concurrecy: {concurrecy:<2} ",
                    f"Jobs: {N:<5} ",
                    f"JPS: {(jps)/1_000:.1f}k",
                )

            if timedelta(seconds=0) < args.time_limit < datetime.now() - start:
                return


if __name__ == "__main__":
    asyncio.run(main())
