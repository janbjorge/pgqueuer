import argparse
import asyncio
import contextlib
import random
import time
from datetime import timedelta
from itertools import count
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


async def consumer(qm: QueueManager, batch_size: int) -> float:
    cnt = 0

    @qm.entrypoint("asyncfetch")
    async def asyncfetch(job: Job) -> None:
        nonlocal cnt
        cnt += 1
        if job.payload is None:
            qm.alive = False

    @qm.entrypoint("syncfetch")
    def syncfetch(job: Job) -> None:
        nonlocal cnt
        cnt += 1
        if job.payload is None:
            qm.alive = False

    with execution_timer() as elapsed:
        await qm.run(batch_size=batch_size)

    return cnt / elapsed().total_seconds()


async def producer(
    alive: asyncio.Event,
    queries: Queries,
    batch_size: int,
    cnt: count,
) -> None:
    entrypoints = ["syncfetch", "asyncfetch"] * batch_size
    while not alive.is_set():
        random.shuffle(entrypoints)
        payloads = [f"{next(cnt)}".encode() for _ in range(batch_size)]
        await queries.enqueue(entrypoints[:batch_size], payloads, [0] * batch_size)


async def main() -> None:
    parser = argparse.ArgumentParser(description="PGQueuer benchmark tool.")

    parser.add_argument(
        "-t",
        "--timer",
        type=lambda x: timedelta(seconds=float(x)),
        default=timedelta(seconds=30),
        help="Run the benchmark for a specified number of seconds. Default is 30.",
    )

    parser.add_argument(
        "-dc",
        "--dequeue",
        type=int,
        default=2,
        help="Number of concurrent dequeue workers. Default is 2.",
    )
    parser.add_argument(
        "-debs",
        "--dequeue-batch-size",
        type=int,
        default=10,
        help="Batch size for dequeue workers. Default is 10.",
    )

    parser.add_argument(
        "-eq",
        "--enqueue",
        type=int,
        default=1,
        help="Number of concurrent enqueue workers. Default is 1.",
    )
    parser.add_argument(
        "-ecbs",
        "--enqueue-batch-size",
        type=int,
        default=15,
        help="Batch size for enqueue workers. Default is 15.",
    )
    args = parser.parse_args()

    print(f"""Settings:
Timer:                  {args.timer.total_seconds()} seconds
Dequeue:                {args.dequeue}
Dequeue Batch Size:     {args.dequeue_batch_size}
Enqueue:                {args.enqueue}
Enqueue Batch Size:     {args.enqueue_batch_size}
""")

    async with asyncpg.create_pool() as pool:
        queries = Queries(pool)
        await queries.clear_log()
        await queries.clear_queue()
        alive = asyncio.Event()

        async def enqueue() -> None:
            cnt = count()
            batch_size = int(args.enqueue_batch_size)

            await asyncio.gather(
                *[
                    producer(alive, queries, batch_size, cnt)
                    for _ in range(int(args.enqueue))
                ]
            )

        async def dequeue() -> list[float]:
            bs = int(args.dequeue_batch_size)
            qms = [QueueManager(pool) for _ in range(int(args.dequeue))]

            async def alive_waiter() -> None:
                await alive.wait()
                for q in qms:
                    q.alive = False

            dequeue_tasks = [consumer(q, bs) for q in qms] + [alive_waiter()]
            return [v for v in await asyncio.gather(*dequeue_tasks) if v is not None]

        async def timer() -> None:
            await asyncio.sleep(args.timer.total_seconds())
            alive.set()

        async def qsize() -> None:
            while not alive.is_set():
                print(f"Queue size: {sum(x.count for x in await queries.queue_size())}")
                await asyncio.sleep(args.timer.total_seconds() / 10)

        jps_arr, *_ = await asyncio.gather(dequeue(), enqueue(), timer(), qsize())
        print(f"Jobs per Second: {sum(jps_arr)/1_000:.2f}k")


if __name__ == "__main__":
    asyncio.run(main())
