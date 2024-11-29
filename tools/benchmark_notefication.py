from __future__ import annotations

import argparse
import asyncio
import random
import signal
from contextlib import suppress
from datetime import timedelta
from itertools import count

from pgqueuer.cli import querier
from pgqueuer.db import dsn
from pgqueuer.listeners import initialize_notice_event_listener
from pgqueuer.models import PGChannel
from pgqueuer.queries import DBSettings, Queries


async def producer(
    shutdown: asyncio.Event,
    queries: Queries,
    batch_size: int,
    cnt: count,
) -> None:
    assert batch_size > 0
    entrypoints = ["<tools/benchmark_notefication>"] * batch_size
    while not shutdown.is_set():
        await queries.enqueue(
            random.sample(entrypoints, k=batch_size),
            [f"{next(cnt)}".encode() for _ in range(batch_size)],
            [0] * batch_size,
        )


def cli_parser() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="PGQueuer benchmark tool.")

    parser.add_argument(
        "-d",
        "--driver",
        default="apg",
        help="Postgres driver to be used asyncpg (apg) or psycopg (psy).",
        choices=["apg", "apgpool", "psy"],
    )

    parser.add_argument(
        "-t",
        "--timer",
        type=lambda x: timedelta(seconds=float(x)),
        default=timedelta(seconds=30),
        help="Run the benchmark for a specified number of seconds. Default is 10.",
    )

    parser.add_argument(
        "-eq",
        "--enqueue",
        type=int,
        default=1,
        help="Number of concurrent enqueue tasks. Default is 1.",
    )
    parser.add_argument(
        "-eqbs",
        "--enqueue-batch-size",
        type=int,
        default=10,
        help="Batch size for enqueue tasks. Default is 10.",
    )

    parser.add_argument(
        "-o",
        "--output-json",
        type=str,
        default=None,
        help="Path to the output JSON file for benchmark metrics.",
    )
    return parser.parse_args()


async def main(args: argparse.Namespace) -> None:
    print("\n========== PGQueuer Benchmark Settings ==========")
    print(f"Timer:                {args.timer.total_seconds():>10} seconds")
    print(f"Enqueue Tasks:        {args.enqueue:>10}")
    print(f"Enqueue Batch Size:   {args.enqueue_batch_size:>10}")
    print("==================================================\n")

    await (await querier(args.driver, dsn())).clear_log()
    await (await querier(args.driver, dsn())).clear_queue()

    latencies = list[timedelta]()
    shutdown = asyncio.Event()

    async def enqueue() -> None:
        cnt = count()
        producers = [
            producer(
                shutdown,
                await querier(args.driver, dsn()),
                int(args.enqueue_batch_size),
                cnt,
            )
            for _ in range(args.enqueue)
        ]
        await asyncio.gather(*producers)

    async def dequeue() -> None:
        connection = (await querier(args.driver, dsn())).driver
        await initialize_notice_event_listener(
            connection,
            PGChannel(DBSettings().channel),
            lambda x: latencies.append(x.root.latency),
        )
        await shutdown.wait()

    async def dequeue_shutdown_timer() -> None:
        _, pending = await asyncio.wait(
            (
                asyncio.create_task(asyncio.sleep(args.timer.total_seconds())),
                asyncio.create_task(shutdown.wait()),
            ),
            return_when=asyncio.FIRST_COMPLETED,
        )
        # Stop producers
        shutdown.set()

        for p in pending:
            p.cancel()

    loop = asyncio.get_running_loop()
    loop.add_signal_handler(signal.SIGINT, shutdown.set)  # Handle Ctrl-C
    loop.add_signal_handler(signal.SIGTERM, shutdown.set)  # Handle termination request

    await asyncio.gather(
        dequeue(),
        enqueue(),
        dequeue_shutdown_timer(),
    )

    from statistics import mean, median

    print("\n========== Benchmark Results ==========")
    if latencies:
        print(f"Number of Samples:   {len(latencies):>10}")
        print(f"Min Latency:         {min(x.total_seconds() for x in latencies):>10.6f} seconds")
        print(f"Mean Latency:        {mean(x.total_seconds() for x in latencies):>10.6f} seconds")
        print(f"Median Latency:      {median(x.total_seconds() for x in latencies):>10.6f} seconds")
        print(f"Max Latency:         {max(x.total_seconds() for x in latencies):>10.6f} seconds")
    else:
        print("No latency data collected.")
    print("========================================\n")


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main(cli_parser()))
