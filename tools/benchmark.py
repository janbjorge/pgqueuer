import argparse
import asyncio
import random
from datetime import timedelta
from itertools import count

from PgQueuer.cli import querier
from PgQueuer.db import dsn
from PgQueuer.models import Job
from PgQueuer.qm import QueueManager
from PgQueuer.queries import Queries
from tqdm import tqdm


async def consumer(qm: QueueManager, batch_size: int, bar: tqdm) -> None:
    @qm.entrypoint("asyncfetch")
    async def asyncfetch(job: Job) -> None:
        bar.update()
        if job.payload is None:
            qm.alive = False

    @qm.entrypoint("syncfetch")
    def syncfetch(job: Job) -> None:
        bar.update()
        if job.payload is None:
            qm.alive = False

    await qm.run(batch_size=batch_size)


async def producer(
    alive: asyncio.Event,
    queries: Queries,
    batch_size: int,
    cnt: count,
) -> None:
    assert batch_size > 0
    entrypoints = ["syncfetch", "asyncfetch"] * batch_size
    while not alive.is_set():
        random.shuffle(entrypoints)
        payloads = [f"{next(cnt)}".encode() for _ in range(batch_size)]
        await queries.enqueue(entrypoints[:batch_size], payloads, [0] * batch_size)


async def main() -> None:
    parser = argparse.ArgumentParser(description="PGQueuer benchmark tool.")

    parser.add_argument(
        "-d",
        "--driver",
        default="apg",
        help="Postgres driver to be used asyncpg (apg) or psycopg (psy).",
        choices=["apg", "psy"],
    )

    parser.add_argument(
        "-t",
        "--timer",
        type=lambda x: timedelta(seconds=float(x)),
        default=timedelta(seconds=10),
        help="Run the benchmark for a specified number of seconds. Default is 10.",
    )

    parser.add_argument(
        "-dq",
        "--dequeue",
        type=int,
        default=5,
        help="Number of concurrent dequeue workers. Default is 5.",
    )
    parser.add_argument(
        "-dqbs",
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
        "-eqbs",
        "--enqueue-batch-size",
        type=int,
        default=10,
        help="Batch size for enqueue workers. Default is 10.",
    )
    args = parser.parse_args()

    print(f"""Settings:
Timer:                  {args.timer.total_seconds()} seconds
Dequeue:                {args.dequeue}
Dequeue Batch Size:     {args.dequeue_batch_size}
Enqueue:                {args.enqueue}
Enqueue Batch Size:     {args.enqueue_batch_size}
""")

    util_queries = await querier(args.driver, dsn())

    await util_queries.clear_log()
    await util_queries.clear_queue()

    alive = asyncio.Event()

    async def enqueue() -> None:
        cnt = count()

        queries = [await querier(args.driver, dsn()) for _ in range(args.enqueue)]

        await asyncio.gather(
            *[producer(alive, q, int(args.enqueue_batch_size), cnt) for q in queries]
        )

    async def dequeue() -> None:
        queries = [await querier(args.driver, dsn()) for _ in range(args.dequeue)]
        qms = [QueueManager(q.driver) for q in queries]

        async def alive_waiter() -> None:
            await alive.wait()
            for q in qms:
                q.alive = False

        with tqdm(ascii=True, unit=" job", unit_scale=True) as bar:
            dequeue_tasks = [
                consumer(q, int(args.dequeue_batch_size), bar) for q in qms
            ] + [alive_waiter()]

            await asyncio.gather(*dequeue_tasks)

    async def alive_timer() -> None:
        await asyncio.sleep(args.timer.total_seconds())
        alive.set()

    await asyncio.gather(dequeue(), enqueue(), alive_timer())
    print(f"Queue size: {sum(x.count for x in await util_queries.queue_size())}")


if __name__ == "__main__":
    asyncio.run(main())
