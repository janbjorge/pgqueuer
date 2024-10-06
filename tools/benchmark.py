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
from typing import Literal

from pydantic import AwareDatetime, BaseModel
from tqdm.asyncio import tqdm

from pgqueuer.cli import querier
from pgqueuer.db import dsn
from pgqueuer.models import Job
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries


class BenchmarkResult(BaseModel):
    created_at: AwareDatetime
    driver: Literal["apg", "apgpool", "psy"]
    elapsed: timedelta
    github_ref_name: str
    rate: float
    steps: int


async def consumer(
    qm: QueueManager,
    batch_size: int,
    entrypoint_rps: list[float],
    concurrency_limits: list[int],
    bar: tqdm,
) -> None:
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
    alive: asyncio.Event,
    queries: Queries,
    batch_size: int,
    cnt: count,
) -> None:
    assert batch_size > 0
    entrypoints = ["syncfetch", "asyncfetch"] * batch_size
    while not alive.is_set():
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
        help="RPS for endporints given as a list, defautl is 'inf'.",
    )
    parser.add_argument(
        "-ci",
        "--concurrency-limit",
        nargs="+",
        default=[sys.maxsize, sys.maxsize],
        help=f"Concurrency limit for endporints given as a list, defautl is '{sys.maxsize}'.",
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
    print(f"""Settings:
Timer:                  {args.timer.total_seconds()} seconds
Dequeue:                {args.dequeue}
Dequeue Batch Size:     {args.dequeue_batch_size}
Enqueue:                {args.enqueue}
Enqueue Batch Size:     {args.enqueue_batch_size}
""")

    await (await querier(args.driver, dsn())).clear_log()
    await (await querier(args.driver, dsn())).clear_queue()

    alive = asyncio.Event()
    qms = list[QueueManager]()
    tqdm_format_dict = {}

    async def enqueue(alive: asyncio.Event) -> None:
        cnt = count()
        producers = [
            producer(
                alive,
                await querier(args.driver, dsn()),
                int(args.enqueue_batch_size),
                cnt,
            )
            for _ in range(args.enqueue)
        ]
        await asyncio.gather(*producers)

    async def dequeue(qms: list[QueueManager]) -> None:
        queries = [await querier(args.driver, dsn()) for _ in range(args.dequeue)]
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

    async def dequeue_alive_timer(
        qms: list[QueueManager],
        alive: asyncio.Event,
    ) -> None:
        _, pending = await asyncio.wait(
            (
                asyncio.create_task(asyncio.sleep(args.timer.total_seconds())),
                asyncio.create_task(alive.wait()),
            ),
            return_when=asyncio.FIRST_COMPLETED,
        )
        # Stop producers
        alive.set()
        # Stop consumers
        for q in qms:
            q.alive.set()

        for p in pending:
            p.cancel()

    def graceful_shutdown(signum: int, frame: object) -> None:
        alive.set()
        for qm in qms:
            qm.alive.set()

    signal.signal(signal.SIGINT, graceful_shutdown)  # Handle Ctrl-C
    signal.signal(signal.SIGTERM, graceful_shutdown)  # Handle termination request

    await asyncio.gather(
        dequeue(qms),
        enqueue(alive),
        dequeue_alive_timer(qms, alive),
    )

    qsize = await (await querier(args.driver, dsn())).queue_size()
    print("Queue size:")
    for status, items in groupby(sorted(qsize, key=lambda x: x.status), key=lambda x: x.status):
        print(f"  {status} {sum(x.count for x in items)}")

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


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        asyncio.run(main(cli_parser()))
