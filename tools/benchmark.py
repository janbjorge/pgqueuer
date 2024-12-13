from __future__ import annotations

import asyncio
import json
import os
import random
import signal
import sys
from contextlib import suppress
from datetime import datetime, timedelta, timezone
from enum import Enum
from itertools import count
from pathlib import Path
from statistics import median

import typer
from pydantic import AwareDatetime, BaseModel
from tabulate import tabulate
from tqdm.asyncio import tqdm

from pgqueuer.db import AsyncpgDriver, AsyncpgPoolDriver, PsycopgDriver, dsn
from pgqueuer.listeners import initialize_notice_event_listener
from pgqueuer.models import EVENT_TYPES, Job, PGChannel
from pgqueuer.qb import DBSettings, add_prefix
from pgqueuer.qm import QueueManager
from pgqueuer.queries import Queries

app = typer.Typer()


class DriverEnum(str, Enum):
    apg = "apg"
    apgpool = "apgpool"
    psy = "psy"


class Latency(BaseModel):
    min: timedelta
    median: timedelta
    max: timedelta

    @staticmethod
    def from_samples(arr: list[float]) -> Latency:
        return Latency(
            min=timedelta(seconds=min(arr)),
            median=timedelta(seconds=median(arr)),
            max=timedelta(seconds=max(arr)),
        )


class BenchmarkResult(BaseModel):
    """Benchmark metrics including driver info, elapsed time, and rate."""

    created_at: AwareDatetime
    driver: DriverEnum
    elapsed: timedelta
    github_ref_name: str
    latency: Latency
    queued: int
    rate: float
    steps: int

    def pretty_print(self) -> None:
        print(
            tabulate(
                [
                    ["Created At", self.created_at],
                    ["Driver", self.driver],
                    ["Elapsed Time", self.elapsed],
                    ["GitHub Ref Name", self.github_ref_name],
                    ["Rate", f"{self.rate:.2f}"],
                    ["Steps", self.steps],
                    ["Median Latency", f"{self.latency.median}"],
                ],
                headers=["Field", "Value"],
                tablefmt=os.environ.get(add_prefix("TABLEFMT"), "pretty"),
                colalign=("left", "left"),
            )
        )


class Settings(BaseModel):
    driver: DriverEnum = typer.Option(
        DriverEnum.apg,
        help="Postgres driver to use.",
    )
    timer: timedelta = typer.Option(
        30.0,
        help="Run the benchmark for this number of seconds.",
    )
    dequeue: int = typer.Option(
        5,
        help="Number of concurrent dequeue tasks.",
    )
    dequeue_batch_size: int = typer.Option(
        10,
        help="Batch size for dequeue tasks.",
    )
    enqueue: int = typer.Option(
        1,
        help="Number of concurrent enqueue tasks.",
    )
    enqueue_batch_size: int = typer.Option(
        10,
        help="Batch size for enqueue tasks.",
    )
    requests_per_second: list[float] = typer.Option(
        [float("inf"), float("inf")],
        help="RPS for endpoints.",
    )
    concurrency_limit: list[int] = typer.Option(
        [sys.maxsize, sys.maxsize],
        help="Concurrency limit.",
    )
    output_json: Path | None = typer.Option(
        None,
        help="Output JSON file for benchmark metrics.",
    )

    def pretty_print(self) -> None:
        print(
            tabulate(
                [
                    ["Driver", self.driver],
                    ["Timer (s)", self.timer.total_seconds()],
                    ["Dequeue Tasks", self.dequeue],
                    ["Dequeue Batch Size", self.dequeue_batch_size],
                    ["Enqueue Tasks", self.enqueue],
                    ["Enqueue Batch Size", self.enqueue_batch_size],
                    ["Requests Per Second", self.requests_per_second],
                    ["Concurrency Limit", self.concurrency_limit],
                    ["Output JSON", self.output_json or "None"],
                ],
                headers=["Field", "Value"],
                tablefmt=os.environ.get(add_prefix("TABLEFMT"), "pretty"),
                colalign=("left", "left"),
            )
        )


async def make_queries(driver: DriverEnum, conninfo: str) -> Queries:
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


async def benchmark(settings: Settings) -> None:
    settings.pretty_print()

    await (await make_queries(settings.driver, dsn())).clear_log()
    await (await make_queries(settings.driver, dsn())).clear_queue()

    shutdown = asyncio.Event()
    qms = list[QueueManager]()
    latencies = list[tuple[EVENT_TYPES, timedelta]]()
    tqdm_format_dict = {}

    async def enqueue_task(shutdown: asyncio.Event) -> None:
        cnt = count()
        producers = [
            producer(
                shutdown,
                await make_queries(settings.driver, dsn()),
                settings.enqueue_batch_size,
                cnt,
            )
            for _ in range(settings.enqueue)
        ]
        await asyncio.gather(*producers)

    async def dequeue_task(qms: list[QueueManager]) -> None:
        queries = [await make_queries(settings.driver, dsn()) for _ in range(settings.dequeue)]
        for q in queries:
            qms.append(QueueManager(q.driver))

        with tqdm(ascii=True, unit=" job", unit_scale=True, file=sys.stdout) as bar:
            consumers = [
                consumer(
                    qm=q,
                    batch_size=settings.dequeue_batch_size,
                    entrypoint_rps=settings.requests_per_second,
                    concurrency_limits=settings.concurrency_limit,
                    bar=bar,
                )
                for q in qms
            ]
            await asyncio.gather(*consumers)
            tqdm_format_dict.update(bar.format_dict)

    async def measure_latency_task() -> None:
        connection = (await make_queries(settings.driver, dsn())).driver
        await initialize_notice_event_listener(
            connection,
            PGChannel(DBSettings().channel),
            lambda x: latencies.append((x.root.type, x.root.latency)),
        )
        await shutdown.wait()

    async def shutdown_timer() -> None:
        _, pending = await asyncio.wait(
            (
                asyncio.create_task(asyncio.sleep(settings.timer.total_seconds())),
                asyncio.create_task(shutdown.wait()),
            ),
            return_when=asyncio.FIRST_COMPLETED,
        )
        shutdown.set()
        for q in qms:
            q.shutdown.set()
        for p in pending:
            p.cancel()

    def graceful_shutdown() -> None:
        shutdown.set()
        for qm in qms:
            qm.shutdown.set()

    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGINT, graceful_shutdown)
    loop.add_signal_handler(signal.SIGTERM, graceful_shutdown)

    await asyncio.gather(
        dequeue_task(qms), enqueue_task(shutdown), measure_latency_task(), shutdown_timer()
    )

    qsize = await (await make_queries(settings.driver, dsn())).queue_size()

    tce_latencies = [x for e, x in latencies if e == "table_changed_event"]
    benchmark_result = BenchmarkResult(
        created_at=datetime.now(timezone.utc),
        driver=settings.driver,
        elapsed=tqdm_format_dict["elapsed"],
        github_ref_name=os.environ.get("REF_NAME", ""),
        latency=Latency.from_samples([x.total_seconds() for x in tce_latencies]),
        queued=sum(x.count for x in qsize),
        rate=tqdm_format_dict["rate"],
        steps=tqdm_format_dict["n"],
    )

    if settings.output_json:
        with open(settings.output_json, "w") as f:
            json.dump(benchmark_result.model_dump(mode="json"), f)

    benchmark_result.pretty_print()


@app.command()
def main(
    driver: DriverEnum = typer.Option(
        DriverEnum.apg,
        "-d",
        "--driver",
        help="Postgres driver to use.",
    ),
    timer: float = typer.Option(
        30.0,
        "-t",
        "--time",
        help="Run the benchmark for this number of seconds.",
    ),
    dequeue: int = typer.Option(
        5,
        help="Number of concurrent dequeue tasks.",
    ),
    dequeue_batch_size: int = typer.Option(
        10,
        help="Batch size for dequeue tasks.",
    ),
    enqueue: int = typer.Option(
        1,
        help="Number of concurrent enqueue tasks.",
    ),
    enqueue_batch_size: int = typer.Option(
        10,
        help="Batch size for enqueue tasks.",
    ),
    requests_per_second: list[float] = typer.Option(
        [float("inf"), float("inf")],
        help="RPS for endpoints.",
    ),
    concurrency_limit: list[int] = typer.Option(
        [sys.maxsize, sys.maxsize],
        help="Concurrency limit.",
    ),
    output_json: Path | None = typer.Option(
        None,
        help="Output JSON file for benchmark metrics.",
    ),
) -> None:
    """Initialize settings and run the benchmark."""
    settings = Settings(
        driver=driver,
        timer=timer,
        dequeue=dequeue,
        dequeue_batch_size=dequeue_batch_size,
        enqueue=enqueue,
        enqueue_batch_size=enqueue_batch_size,
        requests_per_second=requests_per_second,
        concurrency_limit=concurrency_limit,
        output_json=output_json,
    )
    asyncio.run(benchmark(settings))


if __name__ == "__main__":
    with suppress(KeyboardInterrupt):
        app()
