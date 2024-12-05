from __future__ import annotations

import argparse
import asyncio
import json
import os
import random
import signal
import sys
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from itertools import count, groupby
from statistics import mean, median
from typing import Literal

from pydantic import AwareDatetime, BaseModel
from tqdm.asyncio import tqdm

from pgqueuer.db import AsyncpgDriver, AsyncpgPoolDriver, PsycopgDriver, dsn
from pgqueuer.listeners import initialize_notice_event_listener
from pgqueuer.models import EVENT_TYPES, Job, PGChannel
from pgqueuer.qb import DBSettings
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


class BenchmarkResult(BaseModel):
    """Benchmark metrics including driver info, elapsed time, and rate."""

    created_at: AwareDatetime
    driver: Literal["apg", "apgpool", "psy"]
    elapsed: timedelta
    github_ref_name: str
    rate: float
    steps: int


async def make_queries(driver: Literal["psy", "apg", "apgpool"], conninfo: str) -> Queries:
    """Create a Queries instance for the specified PostgreSQL driver."""
    match driver:
        case "apg":
            import asyncpg

            return Queries(AsyncpgDriver(await asyncpg.connect(dsn=conninfo)))
        case "apgpool":
            import asyncpg

            pool = await asyncpg.create_pool(dsn=conninfo)
            assert pool is not None
            return Queries(AsyncpgPoolDriver(pool))

        case "psy":
            import psycopg

            return Queries(
                PsycopgDriver(
                    await psycopg.AsyncConnection.connect(
                        conninfo=conninfo,
                        autocommit=True,
                    )
                )
            )

    raise NotImplementedError(driver)


async def consumer(
    qm: QueueManager,
    batch_size: int,
    entrypoint_rps: list[float],
    concurrency_limits: list[int],
    bar: tqdm,
) -> None:
    """Process jobs from the queue with specified concurrency and rate limits."""
    assert len(entrypoint_rps) == 2
    async_rps, sync_rps = entrypoint_rps
    async_cl, sync_cl = concurrency_limits

    @qm.entrypoint(
        "asyncfetch",
        requests_per_second=async_rps,
        concurrency_limit=async_cl,
    )
    async def asyncfetch(job: Job) -> None:
        bar.update()

    @qm.entrypoint(
        "syncfetch",
        requests_per_second=sync_rps,
        concurrency_limit=sync_cl,
    )
    def syncfetch(job: Job) -> None:
        bar.update()

    await qm.run(batch_size=batch_size)


async def producer(
    shutdown: asyncio.Event,
    queries: Queries,
    batch_size: int,
    cnt: count,
) -> None:
    """Enqueue jobs continuously until a shutdown signal is received."""
    assert batch_size > 0
    entrypoints = ["syncfetch", "asyncfetch"] * batch_size
    while not shutdown.is_set():
        await queries.enqueue(
            random.sample(entrypoints, k=batch_size),
            [f"{next(cnt)}".encode() for _ in range(batch_size)],
            [0] * batch_size,
        )


def cli_parser() -> argparse.Namespace:
    """Parse command-line arguments for the benchmark tool."""
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
        "-dq",
        "--dequeue",
        type=int,
        default=5,
        help="Number of concurrent dequeue tasks. Default is 5.",
    )
    parser.add_argument(
        "-dqbs",
        "--dequeue-batch-size",
        type=int,
        default=10,
        help="Batch size for dequeue tasks. Default is 10.",
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
        "-rps",
        "--requests-per-second",
        nargs="+",
        default=[float("inf"), float("inf")],
        help="RPS for endporints given as a list, default is 'inf'.",
    )
    parser.add_argument(
        "-ci",
        "--concurrency-limit",
        nargs="+",
        default=[sys.maxsize, sys.maxsize],
        help=f"Concurrency limit for endporints given as a list, default is '{sys.maxsize}'.",
    )
    parser.add_argument(
        "-o",
        "--output-json",
        type=str,
        default=None,
        help="Path to the output JSON file for benchmark metrics.",
    )
    parser.add_argument(
        "-i",
        "--latency",
        action="store_true",
        help="Measure and display latency data for table changed events.",
    )
    return parser.parse_args()


async def main(args: argparse.Namespace) -> None:
    """Run the benchmark, managing producers, consumers, and measuring latency."""

    print(f"""Settings:
Timer:                  {args.timer.total_seconds()} seconds
Dequeue:                {args.dequeue}
Dequeue Batch Size:     {args.dequeue_batch_size}
Enqueue:                {args.enqueue}
Enqueue Batch Size:     {args.enqueue_batch_size}
""")

    await (await make_queries(args.driver, dsn())).clear_log()
    await (await make_queries(args.driver, dsn())).clear_queue()

    shutdown = asyncio.Event()
    qms = list[QueueManager]()
    latencies = list[tuple[EVENT_TYPES, timedelta]]()
    tqdm_format_dict = {}

    async def enqueue(shutdown: asyncio.Event) -> None:
        """Start producer tasks to enqueue jobs continuously."""

        cnt = count()
        producers = [
            producer(
                shutdown,
                await make_queries(args.driver, dsn()),
                int(args.enqueue_batch_size),
                cnt,
            )
            for _ in range(args.enqueue)
        ]
        await asyncio.gather(*producers)

    async def dequeue(qms: list[QueueManager]) -> None:
        """Start consumer tasks to dequeue jobs and track progress."""

        queries = [await make_queries(args.driver, dsn()) for _ in range(args.dequeue)]
        for q in queries:
            qms.append(QueueManager(q.driver))

        with tqdm(
            ascii=True,
            unit=" job",
            unit_scale=True,
            file=sys.stdout,
        ) as bar:
            consumers = [
                consumer(
                    qm=q,
                    batch_size=int(args.dequeue_batch_size),
                    entrypoint_rps=[float(x) for x in args.requests_per_second],
                    concurrency_limits=[int(x) for x in args.concurrency_limit],
                    bar=bar,
                )
                for q in qms
            ]
            await asyncio.gather(*consumers)
            tqdm_format_dict.update(bar.format_dict)

    async def dequeue_shutdown_timer(
        qms: list[QueueManager],
        shutdown: asyncio.Event,
    ) -> None:
        """Shutdown consumers and producers after the timer expires."""

        _, pending = await asyncio.wait(
            (
                asyncio.create_task(asyncio.sleep(args.timer.total_seconds())),
                asyncio.create_task(shutdown.wait()),
            ),
            return_when=asyncio.FIRST_COMPLETED,
        )
        # Stop producers
        shutdown.set()
        # Stop consumers
        for q in qms:
            q.shutdown.set()

        for p in pending:
            p.cancel()

    async def measure_latency() -> None:
        """Measure latency for table change events and store results."""
        connection = (await make_queries(args.driver, dsn())).driver
        await initialize_notice_event_listener(
            connection,
            PGChannel(DBSettings().channel),
            lambda x: latencies.append((x.root.type, x.root.latency)),
        )
        await shutdown.wait()

    def graceful_shutdown() -> None:
        """Handle graceful shutdown of all tasks on signal interruption."""
        shutdown.set()
        for qm in qms:
            qm.shutdown.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, graceful_shutdown)
    loop.add_signal_handler(signal.SIGTERM, graceful_shutdown)

    await asyncio.gather(
        dequeue(qms),
        enqueue(shutdown),
        measure_latency(),
        dequeue_shutdown_timer(qms, shutdown),
    )

    qsize = await (await make_queries(args.driver, dsn())).queue_size()
    print("Queue size:")
    for status, items in groupby(sorted(qsize, key=lambda x: x.status), key=lambda x: x.status):
        print(f"  {status} {sum(x.count for x in items)}")
    if not qsize:
        print("  0")

    if tqdm_format_dict and args.output_json:
        with open(args.output_json, "w") as f:
            json.dump(
                BenchmarkResult(
                    driver=args.driver,
                    created_at=datetime.now(timezone.utc),
                    elapsed=tqdm_format_dict["elapsed"],
                    rate=tqdm_format_dict["rate"],
                    steps=tqdm_format_dict["n"],
                    github_ref_name=os.environ.get("REF_NAME", ""),
                ).model_dump(mode="json"),
                f,
            )

    tce_latencies = [x for e, x in latencies if e == "table_changed_event"]
    if tce_latencies and args.latency:
        print("\n========== Benchmark Results ==========")
        print(
            "Number of Samples:   ",
            f"{len(tce_latencies):>10}",
        )
        print(
            "Min Latency:         ",
            f"{min(x.total_seconds() for x in tce_latencies):>10.6f} seconds",
        )
        print(
            "Mean Latency:        ",
            f"{mean(x.total_seconds() for x in tce_latencies):>10.6f} seconds",
        )
        print(
            "Median Latency:      ",
            f"{median(x.total_seconds() for x in tce_latencies):>10.6f} seconds",
        )
        print(
            "Max Latency:         ",
            f"{max(x.total_seconds() for x in tce_latencies):>10.6f} seconds",
        )
        print("========================================\n")
    elif args.latency:
        print("No latency data collected.")


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main(cli_parser()))
